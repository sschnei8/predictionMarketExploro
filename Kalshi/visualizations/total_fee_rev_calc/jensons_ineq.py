import numpy as np
import matplotlib.pyplot as plt

# https://en.wikipedia.org/wiki/Jensen%27s_inequality

# The Fee Function (ignoring constants C and 0.0875 for clarity)
def fee_function(p):
    return p * (1 - p)

# 1. Setup the curve data
x = np.linspace(0, 1, 100)
y = fee_function(x)

# 2. Setup the Example Data
# Let's say we have two contracts with very different prices
p1 = 0.10
p2 = 0.90
avg_p = (p1 + p2) / 2

# Calculate Fees
f1 = fee_function(p1)
f2 = fee_function(p2)

# Your Method: Fee of the Average Price
fee_of_avg = fee_function(avg_p)

# The Correct Method: Average of the Fees
avg_of_fees = (f1 + f2) / 2

# 3. Create the Plot
plt.figure(figsize=(10, 6))

# Plot the main curve
plt.plot(x, y, label='Fee Curve: $p(1-p)$', color='#1f77b4', linewidth=2)

# Plot the two actual transactions
plt.scatter([p1, p2], [f1, f2], color='black', s=100, zorder=5, label='Actual Transactions')

# Plot the straight line connecting them (The "Secant")
# The average of the fees MUST lie on this straight line
plt.plot([p1, p2], [f1, f2], color='black', linestyle='--', alpha=0.5)

# Plot Your Method (The Peak)
plt.scatter([avg_p], [fee_of_avg], color='red', s=150, zorder=5, label='Your Method (Fee of Avg Price)')
plt.vlines(avg_p, 0, fee_of_avg, colors='red', linestyles='dotted')

# Plot The Correct Method (The Middle of the straight line)
plt.scatter([avg_p], [avg_of_fees], color='green', s=150, zorder=5, label='Actual Avg Fee')
plt.hlines(avg_of_fees, 0, avg_p, colors='green', linestyles='dotted')

# Annotate the "Error"
plt.annotate('', xy=(avg_p, fee_of_avg), xytext=(avg_p, avg_of_fees),
             arrowprops=dict(arrowstyle='<->', color='red', lw=2))
plt.text(avg_p + 0.02, (fee_of_avg + avg_of_fees)/2, ' Overestimation\n Gap', color='red', va='center')

# Labels and Styling
plt.title('Why Averaging Inputs Overestimates the Fee (Jensen\'s Inequality)', fontsize=14)
plt.xlabel('Price (p)', fontsize=12)
plt.ylabel('Fee Factor', fontsize=12)
plt.legend()
plt.grid(True, alpha=0.3)
plt.ylim(0, 0.3)

plt.show()