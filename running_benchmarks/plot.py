
"""
*********************************************************************************************
*
*   Most of this code is created by Andreas Titgen from 
*   https://github.com/Tietgen-ITU/advanced-data-systems/blob/main/exercise-01/scripts/sentiment/sentiment-benchmark.py
*   But have been modifyed to work for my bechmarking
*
*********************************************************************************************
"""

import matplotlib.pyplot as plt
import numpy as np
import csv

colors = ['blue', 'green', 'red', 'orange', 'purple', '#FF5733', '#33FF57', '#3357FF', 'cyan', 'magenta']



# Function to create and save a bar chart
def plot_bar(categories, values, title="Bar Chart", xlabel="Categories", ylabel="Values", filename="bar_chart.png"):

    plot_colors = colors[:len(categories)]

    plt.figure(figsize=(8, 6))
    plt.bar(categories, values, color=plot_colors)
    plt.title(title, fontsize=20)
    plt.xlabel(xlabel, fontsize=16)
    plt.ylabel(ylabel, fontsize=16)
    plt.grid(True)
    plt.savefig(f"plots/{filename}", format='png')
    plt.close()  # Close the figure after saving


if __name__ == "__main__":
    data = []

    # Load data from a CSV file
    with open('./benchmark_sentiment_stats.csv', 'r') as file:
        reader = csv.reader(file, delimiter=';')
        for row in reader:
        # Skip rows that do not have exactly 4 elements
            if len(row) != 4:
                continue
            repetition, query_type, elapsed_seconds, elapsed_milli = row
            data.append((int(repetition), query_type, float(elapsed_seconds), int(elapsed_milli)))

        
    train_sql = [elapsed_seconds for _, query_type, elapsed_seconds, _ in data if query_type == "SQL_TRAIN"]
    predict_sql = [elapsed_seconds for _, query_type, elapsed_seconds, _ in data if query_type == "SQL_PREDICT"]
    train_udtf = [elapsed_seconds for _, query_type, elapsed_seconds, _ in data if query_type == "UDTF_TRAIN"]
    predict_udtf = [elapsed_seconds for _, query_type, elapsed_seconds, _ in data if query_type == "UDTF_PREDICT"]

    elapsed_train = [sum(train_udtf)/3, sum(train_sql)/3]

    # Plot a bar chart
    plot_bar(["UDTF", "SQL"], elapsed_train, title="UDTF vs. SQL Elapsed training time", xlabel="Solution Type", ylabel="Elapsed Time (seconds)", filename="train_elapsed_seconds_bar.png")

    elapsed_predict = [sum(predict_udtf)/3, sum(predict_sql)/3]
    plot_bar(["UDTF", "SQL"], elapsed_predict, title="UDTF vs. SQL Elapsed prediction time", xlabel="Solution Type", ylabel="Elapsed Time (seconds)", filename="predict_elapsed_seconds_bar.png")
    
    