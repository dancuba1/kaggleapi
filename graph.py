import matplotlib.pyplot as plt

def plot_engagement_graph(series):
    plt.style.use('ggplot')
    plt.figure(figsize=(10, 5))
    series.head(10).sort_values().plot(kind='barh')
    plt.plot(series, marker='o')
    plt.title('Top 10 Youtube Categories by Engagement Score in the UK')
    plt.xlabel('Average Engagement Score (Likes + Comments + Dislikes)')
    plt.ylabel('Category Name')
    plt.tight_layout()
    plt.show()