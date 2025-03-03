import matplotlib.pyplot as plt
import numpy as np
import psutil

# Load data
timestamps, memory_usage = np.loadtxt("memory_log.txt", unpack=True)

# Normalize timestamps
timestamps -= timestamps[0]

# Get system memory limit (in KB)
total_memory_kb = psutil.virtual_memory().total // 1024  # Convert bytes to KB

# Plot memory usage over time
plt.plot(timestamps, memory_usage, label="Memory Usage (KB)", color="blue")

# Add a horizontal line for the system memory limit
plt.axhline(y=total_memory_kb, color="red", linestyle="--", label=f"System Memory Limit ({total_memory_kb / 1024:.1f} MB)")

# Labels and title
plt.xlabel("Time (seconds)")
plt.ylabel("Memory (KB)")
plt.title("Memory Usage Over Time")
plt.legend()

# Show plot
plt.show()