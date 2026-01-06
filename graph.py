import matplotlib.pyplot as plt
def plot_engagement_graph(series):
    plt.style.use('ggplot')
    plt.figure(figsize=(10, 5))
    
    top15 = series.head(15).sort_values()
    
    top15.plot(kind='barh', color='skyblue')

    plt.title('Top 15 Youtube Categories by Engagement Score in the UK')
    plt.xlabel('Average Engagement Score (Likes + Comments + Dislikes)')
    plt.ylabel('Category Name')
    
    plt.tight_layout()

    plt.show()
