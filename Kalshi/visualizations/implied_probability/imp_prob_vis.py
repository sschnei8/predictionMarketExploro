import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd

# ── Data ──────────────────────────────────────────────────────────────────────
# Expected columns: MONTH, implied_prod_band, total_contracts, band_pct_by_month
raw = pd.read_csv('imp_prob_data.csv')
raw['MONTH'] = pd.to_datetime(raw['MONTH'])

# Strip the trailing '%' and cast to float for the pct labels
raw['band_pct'] = raw['band_pct_by_month'].str.rstrip('%').astype(float)

pivot_vals = (
    raw.pivot_table(index='MONTH', columns='implied_prod_band',
                    values='total_contracts', aggfunc='sum', fill_value=0)
    .sort_index()
)
pivot_pct = (
    raw.pivot_table(index='MONTH', columns='implied_prod_band',
                    values='band_pct', aggfunc='sum', fill_value=0)
    .sort_index()
)

bands  = list(pivot_vals.columns)
dates  = pivot_vals.index.tolist()
n      = len(dates)
x_pos  = np.arange(n)

# ── Colors ────────────────────────────────────────────────────────────────────
BG      = "#0a0a0f"
SURFACE = "#111118"
BORDER  = "#1e1e2e"
MUTED   = "#5a5a78"

# Softer, muted teal-to-slate palette — no near-whites, all tones readable on dark
BAND_COLORS = {
    'a. 80-100%': "#8aafcc",   # soft steel blue (no white)
    'b. 60-80%':  "#6b9eb8",   # slate blue
    'c. 40-60%':  "#4d9090",   # muted cyan-teal
    'd. 20-40%':  "#2e6060",   # mid teal
    'e. 0-20%':   "#1e3a3a",   # dark forest teal
}
fallback_palette = ["#1e3a3a", "#2e6060", "#4d9090", "#6b9eb8", "#8aafcc"]
colors = [BAND_COLORS.get(b, fallback_palette[i % len(fallback_palette)])
          for i, b in enumerate(bands)]

# Lighter tones of each band color for the in-bar text labels
LABEL_COLORS = {
    'a. 0-20%':   "#e47893",
    'b. 20-40%':  "#c96c83",
    'c. 40-60%':  "#cb5c77",
    'd. 60-80%':  "#ca506f",
    'e. 80-100%': "#cb3e61",
}
label_colors = [LABEL_COLORS.get(b, "#e8e8f0") for b in bands]

def clean_label(b):
    return b.split('. ', 1)[-1] if '. ' in b else b

# ── Figure ────────────────────────────────────────────────────────────────────
fig, ax = plt.subplots(figsize=(13, 5.2), facecolor=BG)
ax.set_facecolor(SURFACE)

ax.yaxis.set_major_locator(plt.MaxNLocator(6))
ax.grid(axis="y", color=BORDER, linewidth=0.6, linestyle=(0, (2, 4)), zorder=0)
ax.set_axisbelow(True)

# ── Stacked bars ──────────────────────────────────────────────────────────────
bar_width = 0.78
bottoms   = np.zeros(n)
segments  = []   # (xi, bottom, val, pct, label_color)

for band, color, lc in zip(bands, colors, label_colors):
    vals = pivot_vals[band].values.astype(float)
    pcts = pivot_pct[band].values.astype(float)
    ax.bar(x_pos, vals, bottom=bottoms,
           width=bar_width, color=color,
           zorder=3, linewidth=0, label=clean_label(band))
    for xi, (v, p, bot) in enumerate(zip(vals, pcts, bottoms)):
        segments.append((xi, bot, v, p, lc))
    bottoms += vals

# Top-cap line
for xi, total in enumerate(bottoms):
    ax.plot([xi - bar_width/2, xi + bar_width/2], [total, total],
            color=colors[-1], linewidth=0.8, alpha=0.5, zorder=4,
            solid_capstyle="round")

# ── Axes ──────────────────────────────────────────────────────────────────────
tick_step = max(1, n // 10)
tick_idx  = list(range(0, n, tick_step))
ax.set_xticks(tick_idx)
ax.set_xticklabels(
    [dates[i].strftime("'%y %m") for i in tick_idx],
    color=MUTED, fontsize=8.5, fontfamily="monospace"
)
ax.set_xlim(-0.7, n - 0.3)

y_max = bottoms.max() * 1.10
ax.set_ylim(0, y_max)
ax.yaxis.set_tick_params(labelcolor=MUTED, labelsize=8.5)
ax.yaxis.set_major_formatter(mticker.FuncFormatter(
    lambda x, _: f"{x/1e9:.1f}B" if x >= 1e9
             else f"{x/1e6:.0f}M" if x >= 1e6
             else f"{x/1e3:.0f}K" if x >= 1e3
             else str(int(x))
))
for lbl in ax.get_yticklabels():
    lbl.set_fontfamily("monospace")

for spine in ax.spines.values():
    spine.set_visible(False)
ax.tick_params(axis="both", length=0)

# ── Percentage labels (only where segment is tall enough) ─────────────────────
MIN_HEIGHT = y_max * 0.04   # must be >= 4% of y range to get a label

for (xi, bot, val, pct, lc) in segments:
    if val < MIN_HEIGHT or pct < 1.5:
        continue
    ax.text(xi, bot + val / 2, f"{pct:.0f}%",
            ha="center", va="center",
            color=lc, fontsize=6.5, fontfamily="monospace",
            zorder=5, alpha=0.9)

# ── Gold top-edge accent ──────────────────────────────────────────────────────
fig.add_artist(
    plt.Line2D([0.02, 0.98], [1.0, 1.0],
               transform=fig.transFigure,
               color="#c8a96e", linewidth=0.8, alpha=0.5)
)

# ── Legend ────────────────────────────────────────────────────────────────────
handles, labels = ax.get_legend_handles_labels()
leg = ax.legend(
    handles[::-1], labels[::-1],
    title="IMPLIED PROB BUCKETS",
    title_fontsize=8,
    loc="upper left",
    frameon=False,
    fontsize=8,
    labelcolor=MUTED,
    handlelength=1.2,
    handletextpad=0.6,
    borderpad=0,
    labelspacing=0.45,
)
leg.get_title().set_color(MUTED)
leg.get_title().set_fontfamily("monospace")
for text in leg.get_texts():
    text.set_fontfamily("monospace")

# ── Title ─────────────────────────────────────────────────────────────────────
ax.set_title(
    "TOTAL CONTRACTS BY IMPLIED PROBABILITY MONTHLY",
    color=MUTED, fontsize=9, fontfamily="monospace",
    loc="left", pad=12
)
fig.text(0.98, 0.97, "Monthly · Prediction Markets",
         ha="right", va="top", color=MUTED, fontsize=8, fontfamily="monospace")

plt.tight_layout(rect=[0, 0, 1, 0.97])
plt.savefig("implied_prob_chart.png", dpi=1080, bbox_inches="tight", facecolor=BG)
print("saved → implied_prob_chart.png")