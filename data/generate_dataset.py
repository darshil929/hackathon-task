import pandas as pd
import random

post_types = ["carousel", "reels", "static_images"]
num_posts = 100

data = {
    "post_id": range(1, num_posts + 1),
    "post_type": [random.choice(post_types) for _ in range(num_posts)],
    "likes": [random.randint(50, 500) for _ in range(num_posts)],
    "shares": [random.randint(10, 100) for _ in range(num_posts)],
    "comments": [random.randint(5, 200) for _ in range(num_posts)],
    "date_posted": pd.date_range(start="2024-01-01", periods=num_posts).to_list(),
}

df = pd.DataFrame(data)

csv_file_path = "social_media_engagement_data.csv"
df.to_csv(csv_file_path, index=False)

print(f"Dataset created and saved to {csv_file_path}")
