import os, sys
from pathlib import Path
import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import georinex as gr

def plot_histogram(rinex_path):

    timestamps = gr.gettime(rinex_path)
    print(f"Total samples found: {len(timestamps)}")
    data = pd.DataFrame(index=timestamps)
    data['diff'] = data.index.diff().total_seconds()

    fig, axs = plt.subplots(1, 1, tight_layout=True)
    bins = [(x + 0.5) / 10.0 for x in range(0, 50, 1)]
    plot = axs.hist(data, bins=bins)
    fig.suptitle(f'Histogram of seconds between samples for {os.path.basename(rinex_path)}')
    plt.savefig(f"{os.path.basename(rinex_path)}_histogram.png")

if __name__ == '__main__':
    if len(sys.argv)==2:
        rinex_path = sys.argv[1]
        plot_histogram(rinex_path)
    else:
        print("Usage:\n   python rinex_histogram.py path/to/rinexfile")
