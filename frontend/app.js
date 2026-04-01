const fmtInt = new Intl.NumberFormat("zh-CN");
const BASE = window.location.origin;
const dashboardState = {
  home: null,
  aiReports: [],
  deviceSyncState: null,
  sleepQuality: null,
  workoutWeekly: null,
  routeSummaries: [],
  routeDetail: null,
  activeRouteId: null,
};
const chartRegistry = {};
let routeMap = null;
let routeLayerGroup = null;

const MODEL_LABELS = {
  "minimax/minimax-m2.7": "MiniMax M2.7",
  "anthropic/claude-sonnet-4.6": "Claude Sonnet 4.6",
};

const TYPE_NAMES = {
  StepCount: "步数", HeartRate: "心率", ActiveEnergyBurned: "活动能量",
  BasalEnergyBurned: "基础代谢", DistanceWalkingRunning: "步行+跑步距离",
  FlightsClimbed: "爬楼层数", AppleExerciseTime: "锻炼时间",
  AppleStandTime: "站立时间", HeartRateVariabilitySDNN: "心率变异性",
  RestingHeartRate: "静息心率", WalkingHeartRateAverage: "步行平均心率",
  OxygenSaturation: "血氧饱和度", RespiratoryRate: "呼吸频率",
  BodyMass: "体重", Height: "身高", BodyFatPercentage: "体脂率",
  BodyMassIndex: "BMI", LeanBodyMass: "去脂体重",
  VO2Max: "最大摄氧量", BodyTemperature: "体温",
  DistanceCycling: "骑行距离", DistanceSwimming: "游泳距离",
  SwimmingStrokeCount: "游泳划水次数", RunningSpeed: "跑步速度",
  RunningPower: "跑步功率", RunningStrideLength: "跑步步幅",
  RunningGroundContactTime: "触地时间", RunningVerticalOscillation: "垂直振幅",
  WalkingSpeed: "步行速度", WalkingStepLength: "步行步幅",
  WalkingAsymmetryPercentage: "步行不对称", WalkingDoubleSupportPercentage: "双脚支撑",
  AppleWalkingSteadiness: "步行稳定性", EnvironmentalAudioExposure: "环境音量",
  HeadphoneAudioExposure: "耳机音量", SixMinuteWalkTestDistance: "6分钟步行距离",
  PhysicalEffort: "体力消耗", TimeInDaylight: "日光时间",
  AppleSleepingWristTemperature: "睡眠腕温", HeartRateRecoveryOneMinute: "心率恢复(1分钟)",
  AtrialFibrillationBurden: "房颤负担", DistanceWheelchair: "轮椅距离",
  DistanceDownhillSnowSports: "滑雪距离", PushCount: "推动次数",
  SleepAnalysis: "睡眠分析", AudioExposureEvent: "音频暴露事件",
  HandwashingEvent: "洗手事件", HighHeartRateEvent: "高心率事件",
  LowHeartRateEvent: "低心率事件", IrregularHeartRhythmEvent: "心律不齐事件",
  LowCardioFitnessEvent: "低心肺事件",
};

const ACTIVITY_NAMES = {
  Running: "跑步", Walking: "步行", Cycling: "骑行", Swimming: "游泳",
  Hiking: "徒步", Yoga: "瑜伽", FunctionalStrengthTraining: "力量训练",
  TraditionalStrengthTraining: "传统力量训练", HighIntensityIntervalTraining: "HIIT",
  Dance: "舞蹈", Elliptical: "椭圆机", Rowing: "划船", Stairmaster: "爬楼机",
  CoreTraining: "核心训练", Flexibility: "柔韧训练", MindAndBody: "身心训练",
  Pilates: "普拉提", TableTennis: "乒乓球", Badminton: "羽毛球",
  Tennis: "网球", Basketball: "篮球", Soccer: "足球", Volleyball: "排球",
  Golf: "高尔夫", Baseball: "棒球", Softball: "垒球", MartialArts: "武术",
  Boxing: "拳击", Kickboxing: "跆拳道", Climbing: "攀岩", Skiing: "滑雪",
  Snowboarding: "单板滑雪", Surfing: "冲浪", WaterPolo: "水球",
  Skateboarding: "滑板", JumpRope: "跳绳", Cooldown: "放松整理", Other: "其他",
};

const SUM_TYPES = new Set([
  "StepCount", "ActiveEnergyBurned", "BasalEnergyBurned",
  "DistanceWalkingRunning", "DistanceCycling", "DistanceSwimming",
  "DistanceWheelchair", "DistanceDownhillSnowSports",
  "FlightsClimbed", "AppleExerciseTime", "AppleStandTime",
  "SwimmingStrokeCount", "PushCount", "TimeInDaylight",
]);

const METRIC_MAP = {
  steps: { type: "HKQuantityTypeIdentifierStepCount", agg: "sum", title: "今日步数", color: "var(--chart-b)" },
  hr: { type: "HKQuantityTypeIdentifierHeartRate", agg: "avg", title: "今日心率", color: "var(--chart-d)" },
};

const modal = document.getElementById("typeModal");
let currentModalType = null;
let modalDays = 30;
let modalMode = "daily";
let currentHourlyMetric = null;
let currentHourlyDate = "";
let loading = false;
let resizeTimer;
let modalRequestSeq = 0;

function cssVar(name) {
  return getComputedStyle(document.body).getPropertyValue(name).trim();
}

function shortDate(v) { return v ? String(v).slice(5, 10) : "-"; }
function shortDateTime(v) { return v ? String(v).replace("T", " ").slice(0, 16) : "-"; }
function parseDateOnly(v) { return new Date(`${String(v).slice(0, 10)}T12:00:00`); }
function formatLocalDate(date) {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const d = String(date.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}
function dateDaysAgo(days) {
  const d = new Date();
  d.setDate(d.getDate() - days);
  return formatLocalDate(d);
}
function todayStr() { return formatLocalDate(new Date()); }

function fmtNum(v) {
  if (v == null || Number.isNaN(Number(v))) return "-";
  const n = Number(v);
  if (Math.abs(n) >= 1000) return fmtInt.format(Math.round(n));
  if (Math.abs(n) >= 100) return n.toFixed(0);
  if (Math.abs(n) >= 10) return n.toFixed(1);
  return n.toFixed(2);
}

function fmtHours(v) {
  if (v == null || Number.isNaN(Number(v))) return "-";
  return `${Number(v).toFixed(1)} 小时`;
}

function fmtClockMinutes(v) {
  if (v == null || Number.isNaN(Number(v))) return "-";
  const normalized = ((Math.round(Number(v)) % 1440) + 1440) % 1440;
  const hours = String(Math.floor(normalized / 60)).padStart(2, "0");
  const minutes = String(normalized % 60).padStart(2, "0");
  return `${hours}:${minutes}`;
}

function humanMinutes(m) {
  if (m == null || Number.isNaN(Number(m))) return "-";
  const minutes = Number(m);
  if (minutes < 60) return `${Math.round(minutes)} 分钟`;
  return `${(minutes / 60).toFixed(1)} 小时`;
}

function formatDistance(value, unit) {
  if (value == null || Number.isNaN(Number(value))) return "-";
  const normalizedUnit = String(unit || "").toLowerCase();
  const distance = Number(value);
  if (normalizedUnit === "m") return distance >= 1000 ? `${(distance / 1000).toFixed(2)} km` : `${Math.round(distance)} m`;
  if (normalizedUnit === "km") return `${distance.toFixed(2)} km`;
  return `${fmtNum(distance)} ${unit || ""}`.trim();
}

function humanFreshness(hours) {
  if (hours == null) return "未同步";
  if (hours < 1) return "刚同步";
  if (hours < 24) return `${hours.toFixed(1)}h`;
  return `${(hours / 24).toFixed(1)}d`;
}

function displayDeviceName(id) {
  const text = String(id || "").trim();
  if (!text) return "主设备";
  const lower = text.toLowerCase();
  if (lower.includes("iphone")) return "主 iPhone";
  if (lower.includes("watch")) return "主手表";
  return text.length > 24 ? `${text.slice(0, 24)}…` : text;
}

function humanSyncStatus(status) {
  const value = String(status || "").trim().toLowerCase();
  if (!value) return "状态未知";
  if (["ok", "success", "completed"].includes(value)) return "同步正常";
  if (value === "running") return "同步中";
  if (value === "failed") return "同步失败";
  if (value === "stalled") return "等待处理";
  if (value === "partial") return "部分完成";
  if (value === "idle") return "等待同步";
  return String(status);
}

function deltaText(v, positiveLabel = "较前期上升", negativeLabel = "较前期下降") {
  if (v == null) return "暂无可比数据";
  if (Math.abs(v) < 0.1) return "与前期基本持平";
  return `${v > 0 ? positiveLabel : negativeLabel} ${Math.abs(v).toFixed(1)}%`;
}

function average(values) {
  const nums = values.filter(v => v != null && !Number.isNaN(Number(v))).map(Number);
  if (!nums.length) return null;
  return nums.reduce((sum, value) => sum + value, 0) / nums.length;
}

function timeAwareStepTarget(avg7d) {
  const now = new Date();
  const hours = now.getHours() + now.getMinutes() / 60;
  const progress = Math.min(1, Math.max(0.18, (hours - 6) / 16));
  const baseline = avg7d && avg7d > 0 ? avg7d : 7000;
  return Math.max(2200, baseline * progress);
}

function signalClass(level) {
  if (level === "good") return "good";
  if (level === "warn") return "warn";
  if (level === "bad") return "bad";
  return "neutral";
}

function assessSleepSignal(home) {
  const last = Number(home.sleep.last_night_hours || 0);
  if (!last) return { key: "sleep", label: "昨晚睡眠", level: "neutral", value: "暂无数据", note: "同步到睡眠记录后再判断。" };
  const avg7 = Number(home.sleep.avg_7d || 0);
  let level = "good";
  let note = "昨晚恢复质量还可以。";
  if (last < 5.5 || (avg7 && last < avg7 - 1.2)) {
    level = "bad";
    note = "昨晚明显偏短，今天更适合补恢复。";
  } else if (last < 6.5 || (avg7 && last < avg7 - 0.5)) {
    level = "warn";
    note = "恢复略薄，今天别把节奏拉太满。";
  }
  return { key: "sleep", label: "昨晚睡眠", level, value: fmtHours(last), note };
}

function assessActivitySignal(home) {
  const today = Number(home.today.steps || 0);
  const target = timeAwareStepTarget(Number(home.steps.avg_7d || 0));
  let level = "good";
  let note = "按当前时间看，活动进度不错。";
  if (today < target * 0.75) {
    level = "bad";
    note = "今天活动偏少，晚些时候可以补一段走路。";
  } else if (today < target * 1.05) {
    level = "warn";
    note = "活动量还可以，再走一点会更稳。";
  }
  return { key: "activity", label: "今日活动", level, value: `${fmtInt.format(today)} 步`, note };
}

function assessHeartSignal(home) {
  const hr = home.today.heart_rate || {};
  if (hr.avg == null || Number(hr.count || 0) < 40) {
    return { key: "heart", label: "心率记录", level: "neutral", value: "数据较少", note: "今天的心率记录还不够完整。" };
  }
  const baseline = average((home.heart_rate.last_30_days || []).slice(-7).map(item => item.avg_bpm));
  const delta = baseline == null ? null : Math.abs(Number(hr.avg) - baseline);
  let level = "good";
  let note = "和最近几天相比波动不大。";
  if (Number(hr.avg) >= 95 || Number(hr.max) >= 160 || (delta != null && delta > 12)) {
    level = "bad";
    note = "今天心率比近期偏高，先别把节奏拉太满。";
  } else if (Number(hr.avg) >= 88 || Number(hr.max) >= 145 || (delta != null && delta > 7)) {
    level = "warn";
    note = "比近期略高一点，可以继续观察。";
  }
  return { key: "heart", label: "心率波动", level, value: `${fmtNum(hr.avg)} bpm`, note };
}

function assessSyncSignal(home) {
  const hours = home.sync.hours_since_last_sync;
  if (hours == null) return { key: "sync", label: "数据同步", level: "bad", value: "尚未同步", note: "还没有拿到最近数据，今天判断会偏弱。" };
  let level = "good";
  let note = "记录是新的，可以放心看今天状态。";
  if (hours > 24) {
    level = "bad";
    note = "同步有点旧，今天的结论只能作参考。";
  } else if (hours > 8) {
    level = "warn";
    note = "不是最新一轮数据，判断会稍微滞后。";
  }
  return { key: "sync", label: "数据同步", level, value: humanFreshness(hours), note };
}

function assessTodayStatus(home) {
  const signals = [assessSleepSignal(home), assessActivitySignal(home), assessHeartSignal(home), assessSyncSignal(home)];
  const counts = {
    bad: signals.filter(item => item.level === "bad").length,
    warn: signals.filter(item => item.level === "warn").length,
    good: signals.filter(item => item.level === "good").length,
  };
  let overall = { level: "good", title: "今天状态不错", desc: "睡眠、活动和同步完整度都还不错。" };
  if (signals.find(item => item.key === "sleep" && item.level === "bad")) {
    overall = { level: "bad", title: "今天先补恢复", desc: "昨晚睡眠偏短，今天可以把节奏放轻一点。" };
  } else if (counts.bad >= 2) {
    overall = { level: "bad", title: "今天有点偏紧", desc: "有不止一个维度亮了红灯，今天更适合保守一点。" };
  } else if (counts.bad === 1) {
    const focus = signals.find(item => item.level === "bad");
    overall = { level: "warn", title: "今天有一个重点要留意", desc: focus ? focus.note : "今天有一个维度偏弱，先看红灯项。" };
  } else if (counts.warn >= 2) {
    overall = { level: "warn", title: "今天状态一般", desc: "整体还算稳定，但有几个维度只是勉强过线。" };
  }
  return { overall, signals };
}

function greetingText() {
  const hour = new Date().getHours();
  if (hour < 11) return "早上好，先看今天的恢复和活动进度";
  if (hour < 18) return "下午好，关注今天的身体负荷和同步新鲜度";
  return "晚上好，快速回看今天的健康轨迹";
}

function setTopbarDate() {
  const now = new Date();
  const weekday = ["周日", "周一", "周二", "周三", "周四", "周五", "周六"][now.getDay()];
  document.getElementById("currentGreeting").textContent = greetingText();
  document.getElementById("currentDateLabel").textContent = `${now.getFullYear()}年${now.getMonth() + 1}月${now.getDate()}日 · ${weekday}`;
}

async function api(path, options = {}) {
  const res = await fetch(`${BASE}${path}`, options);
  if (!res.ok) throw new Error(`${path} → ${res.status}`);
  const body = await res.json();
  if (body && typeof body === "object" && "data" in body && "meta" in body) {
    return body.data;
  }
  return body;
}

function destroyChart(key) {
  if (chartRegistry[key]) {
    chartRegistry[key].destroy();
    delete chartRegistry[key];
  }
}

function baseChartOptions() {
  return {
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 650, easing: "easeOutQuart" },
    plugins: {
      legend: {
        labels: {
          color: cssVar("--muted"),
          usePointStyle: true,
          boxWidth: 10,
          boxHeight: 10,
          font: { family: "Inter", size: 12, weight: "600" },
        },
      },
      tooltip: {
        backgroundColor: cssVar("--surface"),
        borderColor: cssVar("--border"),
        borderWidth: 1,
        titleColor: cssVar("--text"),
        bodyColor: cssVar("--text"),
        padding: 12,
        titleFont: { family: "Inter", weight: "700" },
        bodyFont: { family: "Inter" },
      },
    },
    scales: {
      x: {
        grid: { display: false },
        ticks: { color: cssVar("--muted"), font: { family: "Inter", size: 11 } },
      },
      y: {
        beginAtZero: true,
        grid: { color: cssVar("--border") },
        ticks: { color: cssVar("--muted"), font: { family: "Inter", size: 11 } },
      },
    },
  };
}

function makeChart(key, canvasId, config) {
  destroyChart(key);
  const ctx = document.getElementById(canvasId);
  if (!ctx) return null;
  chartRegistry[key] = new Chart(ctx, config);
  return chartRegistry[key];
}

function renderStepsChart(items) {
  const labels = items.map(item => {
    const date = parseDateOnly(item.date);
    return `${shortDate(item.date)} 周${["日", "一", "二", "三", "四", "五", "六"][date.getDay()]}`;
  });
  const values = items.map(item => Number(item.steps || 0));
  const options = baseChartOptions();
  options.plugins.legend.display = false;
  options.plugins.tooltip.callbacks = {
    label(context) {
      return `${fmtInt.format(context.parsed.y || 0)} 步`;
    },
  };
  makeChart("steps", "stepsCanvas", {
    type: "bar",
    data: {
      labels,
      datasets: [{
        data: values,
        borderRadius: 12,
        backgroundColor: labels.map((_, idx) => idx === values.length - 1 ? cssVar("--accent") : "rgba(10, 132, 255, 0.45)"),
      }],
    },
    options,
  });
}

function buildSleepStageSeries(rows) {
  const stageMap = {
    HKCategoryValueSleepAnalysisAsleepCore: "浅睡",
    HKCategoryValueSleepAnalysisAsleepDeep: "深睡",
    HKCategoryValueSleepAnalysisAsleepREM: "REM",
    HKCategoryValueSleepAnalysisAsleepUnspecified: "未分期",
    HKCategoryValueSleepAnalysisAwake: "清醒",
  };
  const colors = {
    浅睡: "rgba(10, 132, 255, 0.72)",
    深睡: "rgba(52, 199, 89, 0.72)",
    REM: "rgba(100, 210, 255, 0.72)",
    未分期: "rgba(139, 92, 246, 0.58)",
    清醒: "rgba(255, 149, 0, 0.72)",
  };
  const labels = Array.from({ length: 14 }, (_, idx) => shortDate(dateDaysAgo(13 - idx)));
  const dateKeys = Array.from({ length: 14 }, (_, idx) => dateDaysAgo(13 - idx));
  const byDate = new Map(dateKeys.map(date => [date, { 浅睡: 0, 深睡: 0, REM: 0, 未分期: 0, 清醒: 0 }]));
  (rows || []).forEach(row => {
    const date = String(row.date).slice(0, 10);
    const stage = stageMap[row.stage];
    if (byDate.has(date) && stage) {
      byDate.get(date)[stage] += Number(row.minutes || 0) / 60;
    }
  });
  const datasets = ["深睡", "浅睡", "REM", "未分期", "清醒"].map(name => ({
    label: name,
    data: dateKeys.map(date => byDate.get(date)[name] || 0),
    backgroundColor: colors[name],
    borderRadius: 8,
    borderSkipped: false,
  }));
  return { labels, datasets };
}

function renderSleepChart(home) {
  const series = buildSleepStageSeries(home.sleep.last_14_days_stages || []);
  const options = baseChartOptions();
  options.scales.x.stacked = true;
  options.scales.y.stacked = true;
  options.plugins.tooltip.callbacks = {
    label(context) {
      return `${context.dataset.label}: ${Number(context.parsed.y || 0).toFixed(1)} 小时`;
    },
  };
  makeChart("sleep", "sleepCanvas", {
    type: "bar",
    data: series,
    options,
  });
}

function fillDailySeries(rows, days, valueKey) {
  const map = new Map(rows.map(r => [String(r.date).slice(0, 10), r]));
  const out = [];
  for (let offset = days - 1; offset >= 0; offset -= 1) {
    const date = dateDaysAgo(offset);
    const row = map.get(date);
    out.push({ date, value: row && row[valueKey] != null ? Number(row[valueKey]) : null });
  }
  return out;
}

function renderHeartRateChart(home) {
  const avgSeries = fillDailySeries(home.heart_rate.last_30_days || [], 30, "avg_bpm");
  const minSeries = fillDailySeries(home.heart_rate.last_30_days || [], 30, "min_bpm");
  const maxSeries = fillDailySeries(home.heart_rate.last_30_days || [], 30, "max_bpm");
  const options = baseChartOptions();
  options.scales.y.beginAtZero = false;
  options.plugins.tooltip.callbacks = {
    label(context) {
      return `${context.dataset.label}: ${fmtNum(context.parsed.y)} bpm`;
    },
  };
  makeChart("heart", "hrCanvas", {
    type: "line",
    data: {
      labels: avgSeries.map(item => shortDate(item.date)),
      datasets: [
        {
          label: "最高",
          data: maxSeries.map(item => item.value),
          borderColor: "rgba(255, 149, 0, 0.18)",
          backgroundColor: "rgba(255, 149, 0, 0.06)",
          pointRadius: 0,
          borderWidth: 1,
          fill: false,
        },
        {
          label: "最低-最高区间",
          data: minSeries.map(item => item.value),
          borderColor: "rgba(10, 132, 255, 0.16)",
          backgroundColor: "rgba(10, 132, 255, 0.12)",
          pointRadius: 0,
          borderWidth: 1,
          fill: "-1",
        },
        {
          label: "平均",
          data: avgSeries.map(item => item.value),
          borderColor: cssVar("--chart-d"),
          backgroundColor: cssVar("--chart-d"),
          pointRadius: 0,
          tension: 0.34,
          borderWidth: 2.4,
        },
      ],
    },
    options,
  });
}

function renderSleepQualityPanel(payload) {
  const summaryEl = document.getElementById("sleepQualitySummary");
  const cardsEl = document.getElementById("sleepQualityCards");
  if (!payload || !payload.summary || !Array.isArray(payload.nights) || !payload.nights.length) {
    summaryEl.textContent = "最近没有足够的睡眠结构数据。";
    cardsEl.innerHTML = '<div class="empty-state">暂无睡眠质量数据</div>';
    destroyChart("sleep-quality");
    return;
  }

  const { summary, nights } = payload;
  summaryEl.textContent = `最近 ${summary.nights} 晚平均评分 ${fmtNum(summary.avg_score)}，规律性 ${fmtNum(summary.regularity_score)}，平均睡眠 ${fmtNum(summary.avg_total_hours)} 小时。`;
  cardsEl.innerHTML = `
    <div class="summary-card">
      <div class="summary-label">平均评分</div>
      <div class="summary-value">${fmtNum(summary.avg_score)}</div>
      <div class="summary-note">基于时长、效率、深睡和 REM</div>
    </div>
    <div class="summary-card">
      <div class="summary-label">平均睡眠</div>
      <div class="summary-value">${fmtNum(summary.avg_total_hours)} h</div>
      <div class="summary-note">最近 ${summary.nights} 晚</div>
    </div>
    <div class="summary-card">
      <div class="summary-label">规律性评分</div>
      <div class="summary-value">${fmtNum(summary.regularity_score)}</div>
      <div class="summary-note">平均入睡 ${fmtClockMinutes(summary.avg_bedtime_minutes)} · 波动 ${summary.bedtime_std_minutes == null ? "-" : `${fmtNum(summary.bedtime_std_minutes)} 分钟`}</div>
    </div>
  `;

  const recent = nights.slice(-7);
  const options = baseChartOptions();
  options.plugins.legend.display = false;
  options.scales.y.min = 0;
  options.scales.y.max = 100;
  options.plugins.tooltip.callbacks = {
    label(context) {
      const item = recent[context.dataIndex];
      return [`评分: ${fmtNum(context.parsed.y)}`, `睡眠: ${fmtNum(item.total_hours)} 小时`, `效率: ${item.efficiency == null ? "-" : `${fmtNum(item.efficiency * 100)}%`}`];
    },
  };
  makeChart("sleep-quality", "sleepQualityCanvas", {
    type: "bar",
    data: {
      labels: recent.map(item => shortDate(item.date)),
      datasets: [{
        data: recent.map(item => item.score),
        backgroundColor: recent.map(item => item.score >= 80 ? "rgba(52, 199, 89, 0.72)" : item.score >= 65 ? "rgba(255, 149, 0, 0.72)" : "rgba(255, 59, 48, 0.72)"),
        borderRadius: 10,
      }],
    },
    options,
  });
}

function renderWorkoutWeeklyPanel(payload) {
  const summaryEl = document.getElementById("workoutWeeklySummary");
  const cardsEl = document.getElementById("workoutWeeklyCards");
  if (!payload || !payload.summary || !Array.isArray(payload.weekly) || !payload.weekly.length) {
    summaryEl.textContent = "最近没有训练周报数据。";
    cardsEl.innerHTML = '<div class="empty-state">暂无训练周报</div>';
    destroyChart("workout-weekly");
    return;
  }

  const { summary, weekly, top_types: topTypes } = payload;
  summaryEl.textContent = `最近 ${summary.weeks} 周共 ${summary.total_workouts} 次训练，活跃 ${summary.active_days} 天。`;
  cardsEl.innerHTML = `
    <div class="summary-card">
      <div class="summary-label">总训练次数</div>
      <div class="summary-value">${summary.total_workouts}</div>
      <div class="summary-note">最近 ${summary.weeks} 周</div>
    </div>
    <div class="summary-card">
      <div class="summary-label">总训练时长</div>
      <div class="summary-value">${fmtNum(summary.total_minutes)} min</div>
      <div class="summary-note">平均每周 ${fmtNum(summary.total_minutes / Math.max(summary.weeks, 1))} 分钟</div>
    </div>
    <div class="summary-card">
      <div class="summary-label">最常见训练</div>
      <div class="summary-value">${topTypes && topTypes[0] ? shortActivity(topTypes[0].activity_type) : "-"}</div>
      <div class="summary-note">${topTypes && topTypes[0] ? `${topTypes[0].count} 次` : "暂无"}</div>
    </div>
  `;

  const options = baseChartOptions();
  options.plugins.legend.display = false;
  options.plugins.tooltip.callbacks = {
    label(context) {
      const item = weekly[context.dataIndex];
      return [`${context.parsed.y} 分钟`, `${item.count} 次训练`, `${fmtInt.format(item.calories || 0)} 千卡`];
    },
  };
  makeChart("workout-weekly", "workoutWeeklyCanvas", {
    type: "line",
    data: {
      labels: weekly.map(item => shortDate(item.week_start)),
      datasets: [{
        label: "每周训练分钟",
        data: weekly.map(item => item.minutes),
        borderColor: cssVar("--chart-d"),
        backgroundColor: "rgba(255, 149, 0, 0.12)",
        fill: true,
        pointRadius: 3,
        tension: 0.28,
        borderWidth: 2.4,
      }],
    },
    options,
  });
}

function ensureRouteMap() {
  if (typeof L === "undefined") return null;
  if (!routeMap) {
    routeMap = L.map("routeMap", { zoomControl: true });
    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      attribution: "&copy; OpenStreetMap",
      maxZoom: 19,
    }).addTo(routeMap);
    routeLayerGroup = L.layerGroup().addTo(routeMap);
  }
  return routeMap;
}

function renderRouteCards(detail) {
  const cards = document.getElementById("routeCards");
  cards.innerHTML = `
    <div class="summary-card">
      <div class="summary-label">路线类型</div>
      <div class="summary-value">${shortActivity(detail.activity_type)}</div>
      <div class="summary-note">${shortDate(detail.date || detail.start_at)} · ${humanMinutes(detail.duration)}</div>
    </div>
    <div class="summary-card">
      <div class="summary-label">距离 / 点数</div>
      <div class="summary-value">${formatDistance(detail.total_distance, detail.total_distance_unit)}</div>
      <div class="summary-note">${fmtInt.format(detail.point_count || 0)} 个 GPS 点</div>
    </div>
    <div class="summary-card">
      <div class="summary-label">能量 / 抽样</div>
      <div class="summary-value">${detail.total_energy_burned ? fmtInt.format(Math.round(detail.total_energy_burned)) : "-"} kcal</div>
      <div class="summary-note">地图绘制返回 ${fmtInt.format((detail.sampled_points || []).length)} 点</div>
    </div>
  `;
}

function renderRouteList() {
  const root = document.getElementById("routeList");
  const routes = dashboardState.routeSummaries || [];
  if (!routes.length) {
    root.innerHTML = '<div class="empty-state">还没有带 GPS 轨迹的训练路线。</div>';
    return;
  }
  root.innerHTML = routes.map(item => `
    <article class="route-item ${item.id === dashboardState.activeRouteId ? "active" : ""}" data-route-id="${item.id}">
      <div class="route-item-title">${shortActivity(item.activity_type)}</div>
      <div class="route-item-sub">${shortDate(item.date || item.start_at)} · ${humanMinutes(item.duration)}</div>
      <div class="route-item-meta">${formatDistance(item.total_distance, item.total_distance_unit)} · ${fmtInt.format(item.point_count || 0)} 点</div>
    </article>
  `).join("");
  root.querySelectorAll(".route-item").forEach(node => {
    node.addEventListener("click", () => {
      const workoutId = Number(node.dataset.routeId);
      if (workoutId && workoutId !== dashboardState.activeRouteId) loadRouteDetail(workoutId);
    });
  });
}

function renderRouteEmpty(message) {
  document.getElementById("routeSummary").textContent = message;
  document.getElementById("routeMeta").textContent = "如果后续继续同步 workout route，这里会自动出现。";
  document.getElementById("routeCards").innerHTML = '<div class="empty-state">暂无路线摘要</div>';
  renderRouteList();
  const mapEl = document.getElementById("routeMap");
  if (!routeMap) {
    mapEl.innerHTML = '<div class="empty-state" style="padding: 24px;">暂无可绘制的路线地图</div>';
  } else {
    routeLayerGroup.clearLayers();
  }
}

function renderRouteDetail(detail) {
  if (!detail || !Array.isArray(detail.sampled_points) || !detail.sampled_points.length) {
    renderRouteEmpty("这条训练没有可绘制的路线点。");
    return;
  }
  document.getElementById("routeSummary").textContent = `最近共找到 ${dashboardState.routeSummaries.length} 条带 GPS 的训练路线，当前查看 ${shortActivity(detail.activity_type)}。`;
  document.getElementById("routeMeta").textContent = `${shortDateTime(detail.start_at)} 开始 · ${formatDistance(detail.total_distance, detail.total_distance_unit)} · 抽样步长 ${detail.sample_step || 1}`;
  renderRouteCards(detail);
  renderRouteList();

  const mapEl = document.getElementById("routeMap");
  if (!routeMap) mapEl.innerHTML = "";
  const map = ensureRouteMap();
  if (!map) return;
  routeLayerGroup.clearLayers();

  const latlngs = detail.sampled_points.map(point => [Number(point.latitude), Number(point.longitude)]);
  const polyline = L.polyline(latlngs, { color: cssVar("--accent"), weight: 4, opacity: 0.9 }).addTo(routeLayerGroup);
  L.circleMarker(latlngs[0], {
    radius: 6,
    color: cssVar("--emerald"),
    fillColor: cssVar("--emerald"),
    fillOpacity: 1,
  }).bindTooltip("起点").addTo(routeLayerGroup);
  L.circleMarker(latlngs[latlngs.length - 1], {
    radius: 6,
    color: cssVar("--danger"),
    fillColor: cssVar("--danger"),
    fillOpacity: 1,
  }).bindTooltip("终点").addTo(routeLayerGroup);
  map.fitBounds(polyline.getBounds(), { padding: [18, 18] });
  setTimeout(() => map.invalidateSize(), 0);
}

async function loadRouteDetail(workoutId) {
  dashboardState.activeRouteId = workoutId;
  document.getElementById("routeMeta").textContent = "正在读取路线坐标…";
  renderRouteList();
  try {
    const detail = await api(`/api/workouts/${workoutId}/route?max_points=2500`);
    if (dashboardState.activeRouteId !== workoutId) return;
    dashboardState.routeDetail = detail;
    document.getElementById("routeSelect").value = String(workoutId);
    renderRouteDetail(detail);
  } catch (err) {
    renderRouteEmpty(`路线读取失败: ${err.message}`);
  }
}

function renderRouteOverview(routes) {
  dashboardState.routeSummaries = Array.isArray(routes) ? routes : [];
  const select = document.getElementById("routeSelect");
  if (!dashboardState.routeSummaries.length) {
    select.innerHTML = "<option>暂无路线数据</option>";
    renderRouteEmpty("最近没有带 GPS 轨迹的训练。");
    return;
  }
  select.innerHTML = dashboardState.routeSummaries.map(item => `
    <option value="${item.id}">${shortDate(item.date || item.start_at)} · ${shortActivity(item.activity_type)} · ${formatDistance(item.total_distance, item.total_distance_unit)}</option>
  `).join("");
  const activeId = dashboardState.activeRouteId && dashboardState.routeSummaries.find(item => item.id === dashboardState.activeRouteId)
    ? dashboardState.activeRouteId
    : dashboardState.routeSummaries[0].id;
  dashboardState.activeRouteId = activeId;
  select.value = String(activeId);
  renderRouteList();
  loadRouteDetail(activeId);
}

function renderModalLineChart(labels, values, { color, fill = null, yMin, label = "值", key = "modal-line", tooltipSuffix = "" }) {
  clearModalChart();
  const options = baseChartOptions();
  options.plugins.legend.display = false;
  if (yMin != null) options.scales.y.min = yMin;
  options.plugins.tooltip.callbacks = {
    label(context) {
      return `${label}: ${fmtNum(context.parsed.y)}${tooltipSuffix}`;
    },
  };
  makeChart(key, "modalCanvas", {
    type: "line",
    data: {
      labels,
      datasets: [{
        label,
        data: values,
        borderColor: color,
        backgroundColor: fill || color,
        pointRadius: 0,
        tension: 0.3,
        fill: Boolean(fill),
        borderWidth: 2.4,
      }],
    },
    options,
  });
}

function renderModalBarChart(labels, values, { color, label = "值", tooltipSuffix = "", key = "modal-bar" }) {
  clearModalChart();
  const options = baseChartOptions();
  options.plugins.legend.display = false;
  options.plugins.tooltip.callbacks = {
    label(context) {
      return `${label}: ${fmtNum(context.parsed.y)}${tooltipSuffix}`;
    },
  };
  makeChart(key, "modalCanvas", {
    type: "bar",
    data: {
      labels,
      datasets: [{
        data: values,
        backgroundColor: color,
        borderRadius: 8,
      }],
    },
    options,
  });
}

function clearModalChart() {
  destroyChart("modal-line");
  destroyChart("modal-bar");
}

function renderHero(home) {
  const assessment = assessTodayStatus(home);
  const badge = document.getElementById("heroBadge");
  badge.className = `hero-badge ${signalClass(assessment.overall.level)}`;
  badge.textContent = assessment.overall.level === "good" ? "绿灯为主" : assessment.overall.level === "bad" ? "今天要多留意" : "有几个黄灯";
  document.getElementById("heroTitle").textContent = assessment.overall.title;
  document.getElementById("heroDesc").textContent = assessment.overall.desc;

  document.getElementById("heroSignals").innerHTML = assessment.signals.map(item => `
    <article class="signal-card ${signalClass(item.level)}">
      <div class="signal-head">
        <div>
          <div class="signal-mark"></div>
          <div class="signal-label">${item.label}</div>
        </div>
        <div class="signal-status">${item.level === "good" ? "绿灯" : item.level === "warn" ? "黄灯" : item.level === "bad" ? "红灯" : "待补数据"}</div>
      </div>
      <div class="signal-value">${item.value}</div>
      <div class="signal-note">${item.note}</div>
    </article>
  `).join("");

  const devices = home.sync.devices || [];
  const primaryDevice = devices[0];
  document.getElementById("primaryDeviceLabel").textContent = primaryDevice ? displayDeviceName(primaryDevice.device_id) : "主 iPhone";
  document.getElementById("apiHint").textContent = home.sync.last_sync_at ? `最近更新于 ${shortDateTime(home.sync.last_sync_at)}` : "等待同步数据";
  document.getElementById("healthScoreValue").textContent = home.score && home.score.overall != null
    ? `${fmtNum(home.score.overall)} · ${home.score.label || "状态评分"}`
    : "--";

  const focusItems = [
    {
      title: "最近一次同步",
      value: home.sync.last_sync_at ? shortDateTime(home.sync.last_sync_at) : "还没有同步记录",
      sub: home.sync.hours_since_last_sync == null ? "同步后这里会更新。" : `距现在 ${humanFreshness(home.sync.hours_since_last_sync)}`,
    },
    {
      title: "最近一次训练",
      value: home.workouts.last_workout ? shortActivity(home.workouts.last_workout.activity_type) : "最近 30 天暂无训练",
      sub: home.workouts.last_workout
        ? `${shortDate(home.workouts.last_workout.date || home.workouts.last_workout.start_at)} · ${humanMinutes(home.workouts.last_workout.duration)}`
        : "最近有训练后，这里会显示最近一次训练。",
    },
    {
      title: "最近睡眠窗口",
      value: home.sleep.last_night_hours != null ? fmtHours(home.sleep.last_night_hours) : "暂无睡眠数据",
      sub: home.sleep.last_sleep_start && home.sleep.last_sleep_end
        ? `${shortDate(home.sleep.last_sleep_end)} · ${rangeText(home.sleep.last_sleep_start, home.sleep.last_sleep_end)}`
        : "同步到睡眠记录后，这里会显示最近一晚。",
    },
    {
      title: "主设备状态",
      value: primaryDevice ? humanSyncStatus(primaryDevice.last_sync_status) : "等待同步",
      sub: primaryDevice ? `设备：${displayDeviceName(primaryDevice.device_id)}` : "默认按单设备场景展示。",
    },
  ];

  document.getElementById("focusList").innerHTML = focusItems.map(item => `
    <article class="focus-chip">
      <div class="focus-title">${item.title}</div>
      <div class="focus-value">${item.value}</div>
      <div class="focus-sub">${item.sub}</div>
    </article>
  `).join("");
}

function rangeText(start, end) {
  if (!start || !end) return "-";
  return `${String(start).slice(11, 16)} - ${String(end).slice(11, 16)}`;
}

function renderMetricCards(home) {
  const hr = home.today.heart_rate || {};
  document.getElementById("todaySteps").textContent = fmtInt.format(home.today.steps || 0);
  document.getElementById("todayStepsSub").textContent = `近 7 天总计 ${fmtInt.format(home.steps.total_7d || 0)} · ${deltaText(home.steps.delta_vs_prev_7d, "较前 7 天上升", "较前 7 天下滑")}`;
  document.getElementById("todayHR").textContent = hr.avg != null ? fmtNum(hr.avg) : "-";
  document.getElementById("todayHRSub").textContent = hr.avg != null ? `${fmtNum(hr.min)} ~ ${fmtNum(hr.max)} bpm · ${hr.count || 0} 次采样` : "今日暂无心率数据";
  document.getElementById("sleep7d").textContent = home.sleep.avg_7d != null ? fmtHours(home.sleep.avg_7d) : "-";
  document.getElementById("sleep7dSub").textContent = `昨晚 ${home.sleep.last_night_hours != null ? fmtHours(home.sleep.last_night_hours) : "-"} · ${deltaText(home.sleep.delta_vs_prev_7d, "较前 7 晚更长", "较前 7 晚更短")}`;
  document.getElementById("syncFreshness").textContent = humanFreshness(home.sync.hours_since_last_sync);
  document.getElementById("syncFreshnessSub").textContent = `今日 ${home.sync.today_sync_count || 0} 次同步 · 接受 ${fmtInt.format(home.sync.today_sync_accepted || 0)} 条`;
}

function renderStepsPanel(home) {
  renderStepsChart(home.steps.last_7_days || []);
  document.getElementById("stepsSummary").textContent = `近 7 天日均 ${fmtInt.format(Math.round(home.steps.avg_7d || 0))} 步，${deltaText(home.steps.delta_vs_prev_7d, "比前 7 天更活跃", "比前 7 天更少活动")}`;
}

function renderSleepPanel(home) {
  renderSleepChart(home);
  document.getElementById("sleepSummary").textContent = `近 7 晚平均 ${home.sleep.avg_7d != null ? Number(home.sleep.avg_7d).toFixed(1) : "-"} 小时，${deltaText(home.sleep.delta_vs_prev_7d, "比前 7 晚睡得更多", "比前 7 晚睡得更少")}`;
}

function renderHeartRatePanel(home) {
  renderHeartRateChart(home);
  document.getElementById("hrSummary").textContent = `近 7 天平均心率 ${home.heart_rate.avg_7d != null ? fmtNum(home.heart_rate.avg_7d) : "-"} bpm，${deltaText(home.heart_rate.delta_vs_prev_7d, "较前 7 天更高", "较前 7 天更低")}`;
}

function shortType(t) {
  const key = (t || "").replace("HKQuantityTypeIdentifier", "").replace("HKCategoryTypeIdentifier", "").replace("HKDataType", "");
  return TYPE_NAMES[key] || key || "-";
}

function shortActivity(t) {
  const key = (t || "").replace("HKWorkoutActivityType", "");
  return ACTIVITY_NAMES[key] || key || "-";
}

function renderInsights(home) {
  const root = document.getElementById("insights");
  const rows = home.insights || [];
  if (!rows.length) {
    root.innerHTML = '<div class="empty-state">近期没有明显提醒，可以继续补更细的睡眠和训练规则。</div>';
    return;
  }
  root.innerHTML = rows.map(item => `
    <article class="insight-item ${item.level || "notice"} ${item.raw_type ? "clickable" : ""}" data-type="${item.raw_type || ""}">
      <div class="insight-level">${item.level || "notice"}</div>
      <div class="insight-title">${item.title || "-"}</div>
      <div class="insight-detail">${item.detail || "-"}</div>
    </article>
  `).join("");
  root.querySelectorAll(".insight-item.clickable").forEach(node => {
    node.addEventListener("click", () => openTypeDetail(node.dataset.type));
  });
}

function renderRows(id, rows, render, emptyCols) {
  const el = document.getElementById(id);
  if (!rows || !rows.length) {
    el.innerHTML = `<tr><td colspan="${emptyCols || 4}" class="empty-state">暂无数据</td></tr>`;
    return;
  }
  el.innerHTML = rows.map(r => `<tr>${render(r)}</tr>`).join("");
}

function renderWorkouts(home) {
  const last = home.workouts.last_workout;
  document.getElementById("workoutHighlight").innerHTML = `
    <div class="summary-card">
      <div class="summary-label">最近一次训练</div>
      <div class="summary-value">${last ? shortActivity(last.activity_type) : "-"}</div>
      <div class="summary-note">${last ? `${shortDate(last.date || last.start_at)} · ${humanMinutes(last.duration)}` : "最近 30 天暂无训练"}</div>
    </div>
    <div class="summary-card">
      <div class="summary-label">近 7 天训练</div>
      <div class="summary-value">${home.workouts.count_7d || 0} 次</div>
      <div class="summary-note">${humanMinutes(home.workouts.total_minutes_7d || 0)}</div>
    </div>
    <div class="summary-card">
      <div class="summary-label">近 30 天训练</div>
      <div class="summary-value">${home.workouts.count_30d || 0} 次</div>
      <div class="summary-note">${humanMinutes(home.workouts.total_minutes_30d || 0)} · ${fmtInt.format(home.workouts.total_calories_30d || 0)} 千卡</div>
    </div>
  `;

  renderRows("recentWorkoutTable", home.workouts.recent || [], r => `
    <td>${r.date || shortDate(r.start_at)}</td>
    <td>${shortActivity(r.activity_type)}</td>
    <td>${humanMinutes(r.duration)}</td>
    <td>${r.total_energy_burned ? fmtInt.format(Math.round(r.total_energy_burned)) : "-"}</td>
  `, 4);
}

function renderRecentTypes(home) {
  const el = document.getElementById("typesTable");
  const rows = home.recent_types || [];
  if (!rows.length) {
    el.innerHTML = '<tr><td colspan="3" class="empty-state">最近 7 天暂无活跃记录类型</td></tr>';
    return;
  }
  el.innerHTML = rows.map(r => `
    <tr class="table-clickable" data-raw-type="${r.type}">
      <td title="${r.type}">${shortType(r.type)}</td>
      <td>${fmtInt.format(r.count_7d || 0)}</td>
      <td>${shortDateTime(r.last_at)}</td>
    </tr>
  `).join("");
  el.querySelectorAll(".table-clickable").forEach(tr => {
    tr.addEventListener("click", () => openTypeDetail(tr.dataset.rawType));
  });
}

function statusDotClass(status, hoursSinceLastSync) {
  if (String(status || "").toLowerCase() === "failed" || hoursSinceLastSync > 24) return "bad";
  if (String(status || "").toLowerCase() === "running" || hoursSinceLastSync > 8) return "warn";
  return "";
}

function renderStatus(home) {
  const primaryDevice = home.sync.devices && home.sync.devices[0];
  const deviceLabel = primaryDevice ? displayDeviceName(primaryDevice.device_id) : "主设备";
  const syncLabel = primaryDevice ? humanSyncStatus(primaryDevice.last_sync_status) : "等待同步";
  const levelClass = statusDotClass(primaryDevice && primaryDevice.last_sync_status, home.sync.hours_since_last_sync);
  document.getElementById("backendStatus").innerHTML = `<span class="status-dot ${levelClass}"></span><span>${deviceLabel} · ${syncLabel}</span>`;
}

function renderDevices(home) {
  const root = document.getElementById("deviceList");
  const devices = home.sync.devices || [];
  const primary = devices[0];
  if (!primary) {
    root.innerHTML = '<div class="empty-state">还没有主设备同步记录。</div>';
    return;
  }

  const extras = devices.length > 1 ? devices.slice(1, 3) : [];
  root.innerHTML = `
    <div class="device-hero">
      <div class="device-hero-head">
        <div>
          <div class="summary-label">当前主设备</div>
          <div class="device-name">${displayDeviceName(primary.device_id)}</div>
          <div class="device-note">${humanSyncStatus(primary.last_sync_status)} · 最近同步 ${shortDateTime(primary.last_sync_at)}</div>
        </div>
        <span class="tag ${primary.last_sync_status === "failed" ? "tag-gold" : "tag-blue"}">${humanSyncStatus(primary.last_sync_status)}</span>
      </div>
      <div class="device-meta-grid">
        <div class="summary-card">
          <div class="summary-label">最近接受</div>
          <div class="summary-value">${fmtInt.format(primary.last_accepted_count || 0)} 条</div>
          <div class="summary-note">上一次 bridge 入站</div>
        </div>
        <div class="summary-card">
          <div class="summary-label">最近去重</div>
          <div class="summary-value">${fmtInt.format(primary.last_deduplicated_count || 0)} 条</div>
          <div class="summary-note">重复样本过滤后</div>
        </div>
        <div class="summary-card">
          <div class="summary-label">最近发送</div>
          <div class="summary-value">${shortDateTime(primary.last_sent_at) || "-"}</div>
          <div class="summary-note">客户端上报时间</div>
        </div>
      </div>
    </div>
    ${extras.map(item => `
      <div class="device-mini">
        <div class="summary-label">历史设备</div>
        <div class="summary-value">${displayDeviceName(item.device_id)}</div>
        <div class="summary-note">${humanSyncStatus(item.last_sync_status)} · ${shortDateTime(item.last_sync_at)}</div>
      </div>
    `).join("")}
  `;
}

function renderSyncEvents(syncState) {
  const el = document.getElementById("syncEventTable");
  const rows = syncState && Array.isArray(syncState.recent_events) ? syncState.recent_events : [];
  if (!rows.length) {
    el.innerHTML = '<tr><td colspan="3" class="empty-state">暂无自动同步记录</td></tr>';
    return;
  }
  el.innerHTML = rows.slice(0, 6).map(row => `
    <tr>
      <td>${displayDeviceName(row.device_id)}<div class="panel-subtitle">${humanSyncStatus(row.status)}</div></td>
      <td>${shortDateTime(row.received_at || row.sent_at)}</td>
      <td>${fmtInt.format(row.accepted_count || 0)} 条</td>
    </tr>
  `).join("");
}

function renderAIControls(home) {
  const select = document.getElementById("aiModelSelect");
  const analyzeBtn = document.getElementById("aiAnalyzeBtn");
  const refreshBtn = document.getElementById("aiRefreshBtn");
  const meta = document.getElementById("aiMeta");
  const ai = home.ai || {};
  const models = ai.models || [];
  select.innerHTML = models.map(model => `<option value="${model}" ${model === ai.default_model ? "selected" : ""}>${MODEL_LABELS[model] || model}</option>`).join("");

  const enabled = Boolean(ai.available && models.length);
  select.disabled = !enabled;
  analyzeBtn.disabled = !enabled;
  refreshBtn.disabled = !enabled;
  meta.textContent = enabled ? "选择一种模型后，手动生成一份最近数据总结。" : "AI 总结当前不可用。";
}

function renderAIAnalysis(result) {
  const panel = document.getElementById("aiPanel");
  const meta = document.getElementById("aiMeta");
  if (!result || !result.analysis) {
    panel.innerHTML = '<div class="empty-state">暂无 AI 分析结果。</div>';
    return;
  }
  const analysis = result.analysis;
  const bullets = Array.isArray(analysis.bullets) ? analysis.bullets : [];
  const watchouts = Array.isArray(analysis.watchouts) ? analysis.watchouts : [];
  const nextFocus = Array.isArray(analysis.next_focus) ? analysis.next_focus : [];
  panel.innerHTML = `
    <div class="ai-title">${analysis.title || "智能总结"}</div>
    <div class="ai-summary">${analysis.summary || "-"}</div>
    <div class="ai-section">
      <div class="label">重点</div>
      <ul>${bullets.map(item => `<li>${item}</li>`).join("") || "<li>暂无</li>"}</ul>
    </div>
    <div class="ai-section">
      <div class="label">注意</div>
      <ul>${watchouts.map(item => `<li>${item}</li>`).join("") || "<li>暂无明显风险提示</li>"}</ul>
    </div>
    <div class="ai-section">
      <div class="label">下一步关注</div>
      <ul>${nextFocus.map(item => `<li>${item}</li>`).join("") || "<li>继续观察近期数据变化</li>"}</ul>
    </div>
    <div class="ai-section">
      <div class="label">置信度</div>
      <div>${analysis.confidence || "-"}</div>
    </div>
  `;
  const suffix = result.degraded ? " · 已降级到最近一次可用总结" : "";
  meta.textContent = `生成于 ${shortDateTime(result.generated_at)} · ${MODEL_LABELS[result.model] || result.model}${suffix}`;
}

function renderAIReportHistory(reports) {
  const root = document.getElementById("aiHistory");
  if (!reports || !reports.length) {
    root.innerHTML = '<div class="empty-state">还没有最近总结。</div>';
    return;
  }
  root.innerHTML = reports.map((report, idx) => `
    <article class="history-item" data-report-idx="${idx}">
      <div class="history-top">
        <div class="history-title">${report.analysis && report.analysis.title ? report.analysis.title : "AI 总结"}</div>
        <span class="tag tag-blue">${MODEL_LABELS[report.model] || report.model}</span>
      </div>
      <div class="history-meta">${shortDateTime(report.generated_at)}</div>
      <div class="history-summary">${report.analysis && report.analysis.summary ? report.analysis.summary : "-"}</div>
    </article>
  `).join("");
  root.querySelectorAll(".history-item").forEach(node => {
    node.addEventListener("click", () => {
      const report = dashboardState.aiReports[Number(node.dataset.reportIdx)];
      if (report) renderAIAnalysis(report);
    });
  });
}

async function loadAIReportHistory() {
  try {
    const reports = await api("/api/dashboard/ai-reports?limit=6");
    dashboardState.aiReports = Array.isArray(reports) ? reports : [];
    renderAIReportHistory(dashboardState.aiReports);
  } catch (err) {
    document.getElementById("aiHistory").innerHTML = `<div class="empty-state">最近总结加载失败: ${err.message}</div>`;
  }
}

async function generateAIAnalysis(forceRefresh = false) {
  const home = dashboardState.home;
  if (!home || !home.ai || !home.ai.available) return;
  document.getElementById("aiPanel").innerHTML = '<div class="empty-state">AI 正在分析最近数据…</div>';
  document.getElementById("aiMeta").textContent = "正在生成总结…";
  const model = document.getElementById("aiModelSelect").value || home.ai.default_model;
  try {
    const result = await api("/api/dashboard/ai-analysis", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ model, force_refresh: forceRefresh }),
    });
    renderAIAnalysis(result);
    await loadAIReportHistory();
  } catch (err) {
    document.getElementById("aiPanel").innerHTML = `<div class="empty-state">AI 分析失败: ${err.message}</div>`;
    document.getElementById("aiMeta").textContent = "AI 请求失败。";
  }
}

function unitLabel(unit) {
  const map = {
    "count/min": "次/分", "count": "次", "%": "%", "kcal": "千卡",
    "m": "米", "km": "千米", "min": "分钟", "ms": "毫秒",
    "degC": "°C", "W": "瓦", "m/s": "米/秒", "dBASPL": "分贝",
  };
  return map[unit] || unit || "";
}

function aggForType(rawType) {
  const short = rawType.replace("HKQuantityTypeIdentifier", "").replace("HKCategoryTypeIdentifier", "");
  return SUM_TYPES.has(short) ? "sum" : "avg";
}

function nextModalRequestSeq() {
  modalRequestSeq += 1;
  return modalRequestSeq;
}

function closeModal() {
  clearModalChart();
  modal.classList.remove("open");
  nextModalRequestSeq();
  currentModalType = null;
  currentHourlyMetric = null;
  modalMode = "daily";
  document.getElementById("modalRange").hidden = false;
  document.getElementById("modalHourlyDate").hidden = true;
}

async function openTypeDetail(rawType) {
  modalMode = "daily";
  currentHourlyMetric = null;
  document.getElementById("modalRange").hidden = false;
  document.getElementById("modalHourlyDate").hidden = true;
  currentModalType = rawType;
  modalDays = 30;
  document.getElementById("modalTitle").textContent = shortType(rawType);
  document.getElementById("modalSub").textContent = rawType;
  document.getElementById("modalStats").innerHTML = '<div class="modal-stat"><div class="label">加载中…</div></div>';
  document.querySelectorAll("#modalRange button").forEach(button => button.classList.toggle("active", Number(button.dataset.days) === 30));
  modal.classList.add("open");
  await loadModalData(rawType, modalDays, nextModalRequestSeq());
}

async function loadModalData(rawType, days, requestSeq = nextModalRequestSeq()) {
  if (currentModalType !== rawType) return;
  if (rawType === "HKCategoryTypeIdentifierSleepAnalysis") {
    await loadSleepModal(days, requestSeq);
    return;
  }
  const agg = aggForType(rawType);
  const start = days > 0 ? dateDaysAgo(days) : "";
  const qs = start ? `&start=${start}` : "";
  const data = await api(`/api/records/daily?type=${encodeURIComponent(rawType)}&agg=${agg}${qs}`);
  if (requestSeq !== modalRequestSeq || currentModalType !== rawType || modalMode !== "daily" || modalDays !== days) return;

  const vals = data.map(d => d.value).filter(v => v != null).map(Number);
  const unit = data.length ? unitLabel(data[0].unit) : "";
  const aggLabel = agg === "sum" ? "日合计" : "日均值";
  const totalCount = data.reduce((sum, item) => sum + (item.count || 0), 0);

  if (vals.length) {
    const avg = vals.reduce((a, b) => a + b, 0) / vals.length;
    document.getElementById("modalStats").innerHTML = `
      <div class="modal-stat"><div class="label">${aggLabel}均值</div><div class="val">${fmtNum(avg)} <small>${unit}</small></div></div>
      <div class="modal-stat"><div class="label">${aggLabel}最低</div><div class="val">${fmtNum(Math.min(...vals))} <small>${unit}</small></div></div>
      <div class="modal-stat"><div class="label">${aggLabel}最高</div><div class="val">${fmtNum(Math.max(...vals))} <small>${unit}</small></div></div>
      <div class="modal-stat"><div class="label">覆盖天数</div><div class="val">${data.length}</div></div>
      <div class="modal-stat"><div class="label">原始记录数</div><div class="val">${fmtInt.format(totalCount)}</div></div>
    `;
  } else {
    document.getElementById("modalStats").innerHTML = '<div class="modal-stat"><div class="label">暂无数据</div></div>';
  }

  renderModalLineChart(
    data.map(item => shortDate(item.date)),
    data.map(item => item.value == null ? null : Number(item.value)),
    { color: cssVar("--chart-b"), fill: "rgba(52, 199, 89, 0.12)", yMin: agg === "sum" ? 0 : undefined, label: aggLabel, tooltipSuffix: unit ? ` ${unit}` : "" },
  );
}

async function loadSleepModal(days, requestSeq = nextModalRequestSeq()) {
  const start = days > 0 ? dateDaysAgo(days) : "";
  const qs = start ? `?start=${start}` : "";
  const data = await api(`/api/sleep/daily${qs}`);
  if (
    requestSeq !== modalRequestSeq
    || currentModalType !== "HKCategoryTypeIdentifierSleepAnalysis"
    || modalMode !== "daily"
    || modalDays !== days
  ) return;
  const vals = data.map(d => d.total_hours).filter(v => v != null).map(Number);
  if (vals.length) {
    const last = data[data.length - 1];
    document.getElementById("modalStats").innerHTML = `
      <div class="modal-stat"><div class="label">平均睡眠</div><div class="val">${fmtHours(vals.reduce((a, b) => a + b, 0) / vals.length)}</div></div>
      <div class="modal-stat"><div class="label">最短一晚</div><div class="val">${fmtHours(Math.min(...vals))}</div></div>
      <div class="modal-stat"><div class="label">最长一晚</div><div class="val">${fmtHours(Math.max(...vals))}</div></div>
      <div class="modal-stat"><div class="label">覆盖晚数</div><div class="val">${data.length}</div></div>
      <div class="modal-stat"><div class="label">最近一晚</div><div class="val">${last ? fmtHours(last.total_hours) : "-"}</div></div>
    `;
  } else {
    document.getElementById("modalStats").innerHTML = '<div class="modal-stat"><div class="label">暂无睡眠数据</div></div>';
  }
  renderModalLineChart(
    data.map(item => shortDate(item.date)),
    data.map(item => item.total_hours == null ? null : Number(item.total_hours)),
    { color: cssVar("--accent"), fill: "rgba(10, 132, 255, 0.12)", yMin: 0, label: "睡眠时长", tooltipSuffix: " 小时" },
  );
}

async function openMetricDetail(metric) {
  if (metric === "sleep") {
    await openTypeDetail("HKCategoryTypeIdentifierSleepAnalysis");
    return;
  }
  if (metric === "sync") {
    openSyncDetail();
    return;
  }
  const cfg = METRIC_MAP[metric];
  if (!cfg) return;
  modalMode = "hourly";
  currentModalType = null;
  currentHourlyMetric = metric;
  currentHourlyDate = todayStr();
  document.getElementById("modalTitle").textContent = cfg.title;
  document.getElementById("modalSub").textContent = "每小时分布";
  document.getElementById("modalStats").innerHTML = '<div class="modal-stat"><div class="label">加载中…</div></div>';
  document.getElementById("modalRange").hidden = true;
  document.getElementById("modalHourlyDate").hidden = false;
  document.getElementById("modalDateLabel").textContent = "今天";
  document.getElementById("modalDateNext").disabled = true;
  modal.classList.add("open");
  await loadMetricHourly(metric, currentHourlyDate, nextModalRequestSeq());
}

async function loadMetricHourly(metric, date, requestSeq = nextModalRequestSeq()) {
  const cfg = METRIC_MAP[metric];
  if (!cfg) return;
  try {
    const data = await api(`/api/records/hourly?type=${encodeURIComponent(cfg.type)}&agg=${cfg.agg}&date=${date}`);
    if (requestSeq !== modalRequestSeq || currentHourlyMetric !== metric || currentHourlyDate !== date || modalMode !== "hourly") return;
    const slots = Array(24).fill(0);
    data.forEach(item => { slots[item.hour] = item.value || 0; });
    const totalCount = data.reduce((sum, item) => sum + (item.count || 0), 0);
    const unit = data.length ? unitLabel(data[0].unit) : "";
    const nonZero = slots.filter(v => v > 0);
    if (nonZero.length) {
      const total = cfg.agg === "sum" ? slots.reduce((a, b) => a + b, 0) : null;
      const avg = nonZero.reduce((a, b) => a + b, 0) / nonZero.length;
      const max = Math.max(...nonZero);
      const peakHour = slots.indexOf(max);
      document.getElementById("modalStats").innerHTML = `
        ${total != null ? `<div class="modal-stat"><div class="label">全天合计</div><div class="val">${fmtNum(total)} <small>${unit}</small></div></div>` : ""}
        <div class="modal-stat"><div class="label">${cfg.agg === "sum" ? "小时均值" : "全天均值"}</div><div class="val">${fmtNum(avg)} <small>${unit}</small></div></div>
        <div class="modal-stat"><div class="label">峰值</div><div class="val">${fmtNum(max)} <small>${unit}</small></div></div>
        <div class="modal-stat"><div class="label">峰值时段</div><div class="val">${peakHour}:00</div></div>
        <div class="modal-stat"><div class="label">记录条数</div><div class="val">${fmtInt.format(totalCount)}</div></div>
      `;
    } else {
      document.getElementById("modalStats").innerHTML = '<div class="modal-stat"><div class="label">当日暂无数据</div></div>';
    }
    renderModalBarChart(
      Array.from({ length: 24 }, (_, idx) => `${idx}`),
      slots,
      { color: cfg.color.includes("var(") ? cssVar(cfg.color.slice(4, -1)) : cfg.color, label: cfg.title, tooltipSuffix: unit ? ` ${unit}` : "" },
    );
  } catch (err) {
    document.getElementById("modalStats").innerHTML = `<div class="modal-stat"><div class="label">加载失败: ${err.message}</div></div>`;
  }
}

function openSyncDetail() {
  const home = dashboardState.home;
  nextModalRequestSeq();
  modalMode = "sync";
  currentModalType = null;
  currentHourlyMetric = null;
  document.getElementById("modalTitle").textContent = "同步详情";
  document.getElementById("modalSub").textContent = "主设备状态和最近自动同步记录";
  document.getElementById("modalRange").hidden = true;
  document.getElementById("modalHourlyDate").hidden = true;
  if (!home) {
    document.getElementById("modalStats").innerHTML = '<div class="modal-stat"><div class="label">暂无数据</div></div>';
    modal.classList.add("open");
    return;
  }
  const primary = home.sync.devices && home.sync.devices[0];
  document.getElementById("modalStats").innerHTML = `
    <div class="modal-stat"><div class="label">最后同步</div><div class="val">${shortDateTime(home.sync.last_sync_at) || "-"}</div></div>
    <div class="modal-stat"><div class="label">数据新鲜度</div><div class="val">${humanFreshness(home.sync.hours_since_last_sync)}</div></div>
    <div class="modal-stat"><div class="label">今日同步次数</div><div class="val">${home.sync.today_sync_count || 0}</div></div>
    <div class="modal-stat"><div class="label">今日接受记录</div><div class="val">${fmtInt.format(home.sync.today_sync_accepted || 0)} 条</div></div>
    <div class="modal-stat"><div class="label">主设备</div><div class="val">${primary ? displayDeviceName(primary.device_id) : "-"}</div></div>
    <div class="modal-stat"><div class="label">最近设备状态</div><div class="val">${primary ? humanSyncStatus(primary.last_sync_status) : "-"}</div></div>
  `;
  clearModalChart();
  modal.classList.add("open");
}

function setLoadingState(isLoading) {
  document.body.classList.toggle("is-loading", isLoading);
}

async function loadDashboard() {
  if (loading) return;
  loading = true;
  setLoadingState(true);
  document.getElementById("backendStatus").innerHTML = '<span class="status-dot warn"></span><span>正在加载…</span>';
  try {
    const [home, aiReports, deviceSyncState, sleepQuality, workoutWeekly, routeSummaries] = await Promise.all([
      api("/api/dashboard/home"),
      api("/api/dashboard/ai-reports?limit=6").catch(() => []),
      api("/api/device-sync-state").catch(() => null),
      api(`/api/sleep/quality?start=${dateDaysAgo(13)}`).catch(() => null),
      api("/api/workouts/weekly-summary?weeks=12").catch(() => null),
      api("/api/workouts/routes?limit=12").catch(() => []),
    ]);
    dashboardState.home = home;
    dashboardState.aiReports = Array.isArray(aiReports) ? aiReports : [];
    dashboardState.deviceSyncState = deviceSyncState;
    dashboardState.sleepQuality = sleepQuality;
    dashboardState.workoutWeekly = workoutWeekly;
    renderStatus(home);
    renderHero(home);
    renderMetricCards(home);
    renderStepsPanel(home);
    renderSleepPanel(home);
    renderHeartRatePanel(home);
    renderInsights(home);
    renderWorkouts(home);
    renderRecentTypes(home);
    renderDevices(home);
    renderSyncEvents(deviceSyncState);
    renderSleepQualityPanel(sleepQuality);
    renderWorkoutWeeklyPanel(workoutWeekly);
    renderRouteOverview(routeSummaries);
    renderAIControls(home);
    renderAIReportHistory(dashboardState.aiReports);
    if (dashboardState.aiReports.length) renderAIAnalysis(dashboardState.aiReports[0]);
  } catch (err) {
    document.getElementById("backendStatus").innerHTML = `<span class="status-dot bad"></span><span>${err.message}</span>`;
  } finally {
    loading = false;
    setLoadingState(false);
  }
}

function applyTheme(theme) {
  document.body.dataset.theme = theme;
  localStorage.setItem("mah-theme", theme);
  document.getElementById("themeToggle").textContent = theme === "dark" ? "浅色模式" : "深色模式";
  if (dashboardState.home) {
    renderStepsPanel(dashboardState.home);
    renderSleepPanel(dashboardState.home);
    renderHeartRatePanel(dashboardState.home);
  }
}

function initTheme() {
  const saved = localStorage.getItem("mah-theme");
  const prefersDark = window.matchMedia("(prefers-color-scheme: dark)").matches;
  applyTheme(saved || (prefersDark ? "dark" : "light"));
}

document.getElementById("themeToggle").addEventListener("click", () => {
  applyTheme(document.body.dataset.theme === "dark" ? "light" : "dark");
});

document.getElementById("reloadBtn").addEventListener("click", loadDashboard);
document.getElementById("aiAnalyzeBtn").addEventListener("click", () => generateAIAnalysis(false));
document.getElementById("aiRefreshBtn").addEventListener("click", () => generateAIAnalysis(true));
document.getElementById("routeSelect").addEventListener("change", event => {
  const workoutId = Number(event.target.value);
  if (workoutId) loadRouteDetail(workoutId);
});
document.getElementById("modalClose").addEventListener("click", closeModal);
modal.addEventListener("click", event => { if (event.target === modal) closeModal(); });
document.addEventListener("keydown", event => { if (event.key === "Escape") closeModal(); });

document.querySelectorAll(".metric-card.clickable").forEach(card => {
  card.addEventListener("click", () => openMetricDetail(card.dataset.metric));
});

document.querySelectorAll("#modalRange button").forEach(btn => {
  btn.addEventListener("click", () => {
    if (modalMode !== "daily") return;
    document.querySelectorAll("#modalRange button").forEach(button => button.classList.remove("active"));
    btn.classList.add("active");
    modalDays = Number(btn.dataset.days);
    if (currentModalType) loadModalData(currentModalType, modalDays, nextModalRequestSeq());
  });
});

document.getElementById("modalDatePrev").addEventListener("click", () => {
  const date = new Date(`${currentHourlyDate}T12:00:00`);
  date.setDate(date.getDate() - 1);
  currentHourlyDate = date.toISOString().slice(0, 10);
  const isToday = currentHourlyDate >= todayStr();
  document.getElementById("modalDateLabel").textContent = isToday ? "今天" : currentHourlyDate;
  document.getElementById("modalDateNext").disabled = isToday;
  if (currentHourlyMetric) loadMetricHourly(currentHourlyMetric, currentHourlyDate, nextModalRequestSeq());
});

document.getElementById("modalDateNext").addEventListener("click", () => {
  const today = todayStr();
  if (currentHourlyDate >= today) return;
  const date = new Date(`${currentHourlyDate}T12:00:00`);
  date.setDate(date.getDate() + 1);
  currentHourlyDate = date.toISOString().slice(0, 10);
  const isToday = currentHourlyDate >= today;
  document.getElementById("modalDateLabel").textContent = isToday ? "今天" : currentHourlyDate;
  document.getElementById("modalDateNext").disabled = isToday;
  if (currentHourlyMetric) loadMetricHourly(currentHourlyMetric, currentHourlyDate, nextModalRequestSeq());
});

window.addEventListener("resize", () => {
  clearTimeout(resizeTimer);
  resizeTimer = setTimeout(() => {
    if (dashboardState.home) {
      renderStepsPanel(dashboardState.home);
      renderSleepPanel(dashboardState.home);
      renderHeartRatePanel(dashboardState.home);
    }
    if (dashboardState.sleepQuality) renderSleepQualityPanel(dashboardState.sleepQuality);
    if (dashboardState.workoutWeekly) renderWorkoutWeeklyPanel(dashboardState.workoutWeekly);
    if (routeMap) routeMap.invalidateSize();
    if (modalMode === "hourly" && currentHourlyMetric) loadMetricHourly(currentHourlyMetric, currentHourlyDate, nextModalRequestSeq());
    else if (currentModalType) loadModalData(currentModalType, modalDays, nextModalRequestSeq());
  }, 180);
});

initTheme();
setTopbarDate();
loadDashboard();
