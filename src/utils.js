export function sign(n) {
  return n >= 0 ? "+" : "";
}

export function zLabel(z) {
  const abs = Math.abs(z);
  if (abs < 0.5) return "near normal";
  if (abs < 1.0) return "slightly anomalous";
  if (abs < 1.5) return "anomalous";
  if (abs < 2.0) return "very anomalous";
  return "extreme";
}

export function formatSource(source) {
  return source.source || source.title;
}

export function anomalyColor(val) {
  if (val === null || val === undefined) return "rgba(255,255,255,0.04)";
  const clamped = Math.max(-6, Math.min(6, val));
  const t = clamped / 6;
  if (t < 0) {
    const a = -t;
    return `rgba(59,130,246,${(0.2 + a * 0.7).toFixed(2)})`;
  }
  const a = t;
  return `rgba(239,68,68,${(0.2 + a * 0.7).toFixed(2)})`;
}

export const MONTH_LABELS = [
  "Jan",
  "Feb",
  "Mar",
  "Apr",
  "May",
  "Jun",
  "Jul",
  "Aug",
  "Sep",
  "Oct",
  "Nov",
  "Dec",
];
