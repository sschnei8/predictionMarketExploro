import numpy as np
import matplotlib.pyplot as plt
import math

def calculate_fees(C, P):
    """
    Calculate taker fees from: https://kalshi.com/docs/kalshi-fee-schedule.pdf
    """
    raw_fee = 0.07 * C * P * (1 - P)
    fees = math.ceil(raw_fee * 100) / 100
    return fees

# Total contracts will be at (0 to 10,000)
C_values = np.linspace(0, 10000, 1000)

# Use symmetric P value pairs because fees will be the same between pairs
P_pairs = [
    (0.1, 0.9),
    (0.2, 0.8),
    (0.3, 0.7),
    (0.4, 0.6),
    (0.5, None)  # P=0.5 is the maximum fee 
]


fig = plt.figure(figsize=(14, 8))
gs = fig.add_gridspec(3, 3)
ax_main = fig.add_subplot(gs[:, :2])
ax_inset = fig.add_subplot(gs[0, 2])

# Define colors for each pair
colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']

# Plot the main chart
for i, (P1, P2) in enumerate(P_pairs):
    color = colors[i]
    
    # Plot first P value
    fees = [calculate_fees(C, P1) for C in C_values]
    if P2 is None:
        ax_main.plot(C_values, fees, label=f'P = {P1} (maximum)', 
                    linewidth=3, color=color, linestyle='-')
    else:
        ax_main.plot(C_values, fees, label=f'P = {P1} and P = {P2} (identical)', 
                    linewidth=2.5, color=color, linestyle='-')
        
        # Plot second P value with same color but dashed (they overlap)
        fees2 = [calculate_fees(C, P2) for C in C_values]
        ax_main.plot(C_values, fees2, linewidth=1, color=color, 
                    linestyle='--', alpha=0.7)

ax_main.set_xlabel('C (Value)', fontsize=13, fontweight='bold')
ax_main.set_ylabel('Fees ($)', fontsize=13, fontweight='bold')
ax_main.set_title('Fees vs C: Symmetric Around P = 0.5\nFormula: fees = round_up(0.07 × C × P × (1-P))', 
                  fontsize=15, fontweight='bold', pad=20)
ax_main.legend(loc='upper left', fontsize=10, framealpha=0.95)
ax_main.grid(True, alpha=0.3)

# Add annotation about symmetry
ax_main.text(5000, 150, 'P and (1-P) produce\nidentical fees', 
            fontsize=12, bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8),
            ha='center')

# Create inset showing P×(1-P) curve
P_range = np.linspace(0, 1, 100)
multiplier = [p * (1 - p) for p in P_range]

ax_inset.plot(P_range, multiplier, linewidth=2.5, color='black')
ax_inset.axvline(x=0.5, color='red', linestyle='--', linewidth=1.5, alpha=0.7, label='Max at P=0.5')
ax_inset.set_xlabel('P', fontsize=10, fontweight='bold')
ax_inset.set_ylabel('P × (1-P)', fontsize=10, fontweight='bold')
ax_inset.set_title('Fee Multiplier', fontsize=11, fontweight='bold')
ax_inset.grid(True, alpha=0.3)
ax_inset.set_xlim(0, 1)

# Add symmetry annotations on inset
for p_val in [0.2, 0.3, 0.4]:
    y_val = p_val * (1 - p_val)
    ax_inset.plot([p_val, 1-p_val], [y_val, y_val], 'o-', color='gray', 
                 markersize=5, alpha=0.6)

plt.tight_layout()
plt.show()

# Create a second visualization focusing on the symmetry
fig2, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

# Plot smaller and gives more space to explore symmetry 
# Right plot: Shows the P×(1-P) function more prominently
C_values_zoom = np.linspace(0, 2000, 1000)

for i, (P1, P2) in enumerate(P_pairs):
    color = colors[i]
    fees = [calculate_fees(C, P1) for C in C_values_zoom]
    
    if P2 is None:
        ax1.plot(C_values_zoom, fees, label=f'P = {P1}', 
                linewidth=3, color=color)
    else:
        ax1.plot(C_values_zoom, fees, label=f'P = {P1} = P = {P2}', 
                linewidth=2.5, color=color)

ax1.set_xlabel('C (Value)', fontsize=12, fontweight='bold')
ax1.set_ylabel('Fees ($)', fontsize=12, fontweight='bold')
ax1.set_title('Fees Are Symmetric: P and (1-P) Are Identical', fontsize=14, fontweight='bold')
ax1.legend(loc='upper left', fontsize=11)
ax1.grid(True, alpha=0.3)

P_detailed = np.linspace(0.01, 0.99, 200)
multiplier_detailed = [p * (1 - p) for p in P_detailed]

ax2.plot(P_detailed, multiplier_detailed, linewidth=3, color='darkblue')
ax2.axvline(x=0.5, color='red', linestyle='--', linewidth=2, label='Maximum at P = 0.5')
ax2.axhline(y=0.25, color='red', linestyle='--', linewidth=1, alpha=0.5)

# Annotate symmetric pairs
symmetric_pairs = [(0.2, 0.8), (0.3, 0.7), (0.4, 0.6)]
for p1, p2 in symmetric_pairs:
    y = p1 * (1 - p1)
    ax2.plot([p1, p2], [y, y], 'o-', color='orange', markersize=8, linewidth=2, alpha=0.7)
    ax2.text((p1 + p2) / 2, y + 0.01, f'{y:.3f}', ha='center', fontsize=9, 
            bbox=dict(boxstyle='round', facecolor='yellow', alpha=0.6))

ax2.set_xlabel('P (Probability)', fontsize=12, fontweight='bold')
ax2.set_ylabel('P × (1-P) [Fee Multiplier]', fontsize=12, fontweight='bold')
ax2.set_title('Why Fees Are Symmetric:\nP × (1-P) = (1-P) × P', fontsize=14, fontweight='bold')
ax2.legend(fontsize=11)
ax2.grid(True, alpha=0.3)
ax2.set_xlim(0, 1)

plt.tight_layout()
plt.show()

#plt.savefig('/mnt/user-data/outputs/fees_plot_symmetry_explained.png', dpi=300, bbox_inches='tight')
#print("Symmetry explanation plot saved successfully!")
#
#print("\n✓ Charts created that highlight the symmetry!")
#print("\nKey insight: P × (1-P) = (1-P) × P")
#print("This is why P=0.2 produces the same fees as P=0.8")