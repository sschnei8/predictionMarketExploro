import numpy as np
import matplotlib.pyplot as plt
import os

def calculate_total_fees(volume, p):
    """
    Calculate total fees (maker + taker) for a given volume and probability.

    Formula: fee = (0.0175 * (volume * .5) * p(1-p)) + (0.07 * (volume * .5) * p(1-p))

    Where:
    - 0.0175 = maker fee rate (1.75%)
    - 0.07 = taker fee rate (7%)
    """
    return (0.0175 * (volume * 0.5) * p * (1 - p)) + (0.07 * (volume * 0.5) * p * (1 - p))

# Fixed total volume: $40 billion in contracts
TOTAL_VOLUME = 40_000_000_000

# Generate probability distribution
P_values = np.linspace(0, 1, 1000)
fees = [calculate_total_fees(TOTAL_VOLUME, p) for p in P_values]

# Create main visualization
fig = plt.figure(figsize=(16, 10))
gs = fig.add_gridspec(1, 2, hspace=0.3, wspace=0.3)

# Fee breakdown by component
ax_breakdown = fig.add_subplot(gs[0, 0])
maker_fees = [0.0175 * (TOTAL_VOLUME * 0.5) * p * (1-p) for p in P_values]
taker_fees = [0.07 * (TOTAL_VOLUME * 0.5) * p * (1-p) for p in P_values]

ax_breakdown.plot(P_values, maker_fees, linewidth=2.5, color='blue', label='Maker Fees')
ax_breakdown.plot(P_values, taker_fees, linewidth=2.5, color='orange', label='Taker Fees')
ax_breakdown.plot(P_values, fees, linewidth=2.5, color='darkgreen', linestyle='--', label='Total')
ax_breakdown.axvline(x=0.5, color='red', linestyle='--', linewidth=1.5, alpha=0.5)

ax_breakdown.set_xlabel('P (Probability)', fontsize=12, fontweight='bold')
ax_breakdown.set_ylabel('Fees ($)', fontsize=12, fontweight='bold')
ax_breakdown.set_title('Fee Component Breakdown', fontsize=13, fontweight='bold')
ax_breakdown.legend(fontsize=10)
ax_breakdown.grid(True, alpha=0.3)
ax_breakdown.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x/1e9:.2f}B'))

# Key statistics table
ax_stats = fig.add_subplot(gs[0, 1])
ax_stats.axis('off')

# Calculate key statistics
p_values_of_interest = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
stats_data = []

for p in p_values_of_interest:
    total = calculate_total_fees(TOTAL_VOLUME, p)
    maker = 0.0175 * (TOTAL_VOLUME * 0.5) * p * (1-p)
    taker = 0.07 * (TOTAL_VOLUME * 0.5) * p * (1-p)
    stats_data.append([f'{p:.1f}', f'${total/1e9:.3f}B', f'${maker/1e9:.3f}B', f'${taker/1e9:.3f}B'])

# Create table
table = ax_stats.table(cellText=stats_data,
                      colLabels=['P', 'Total Fees', 'Maker', 'Taker'],
                      cellLoc='center',
                      loc='center',
                      colWidths=[0.15, 0.3, 0.3, 0.3])

table.auto_set_font_size(False)
table.set_fontsize(10)
table.scale(1, 3)

# Style the header
for i in range(4):
    cell = table[(0, i)]
    cell.set_facecolor('#40466e')
    cell.set_text_props(weight='bold', color='white')

# Highlight max row (P=0.5)
for i in range(4):
    cell = table[(5, i)]  # Row 5 is P=0.5
    cell.set_facecolor('#ffeb99')
    cell.set_text_props(weight='bold')

# ax_stats.set_title('Revenue by Probability Value', fontsize=13, fontweight='bold', pad=20)
# 
# plt.suptitle(f'Kalshi Total Revenue Analysis\nAssumed Volume: ${TOTAL_VOLUME/1e9:.1f}B',
#             fontsize=18, fontweight='bold', y=0.98)

plt.tight_layout()

# Save the figure in the same directory as this script
script_dir = os.path.dirname(os.path.abspath(__file__))
output_path = os.path.join(script_dir, 'total_revenue_distribution.png')
plt.savefig(output_path, dpi=300, bbox_inches='tight')
print(f"\n✓ Plot saved to: {output_path}")

plt.show()


