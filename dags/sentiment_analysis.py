def perform_sentiment_analysis(**context):
    """
    Perform French sentiment analysis on reviews stored in PostgreSQL
    """
    from transformers import pipeline, AutoTokenizer
    import pandas as pd
    import psycopg2
    from sqlalchemy import create_engine
    import numpy as np
    
    # Database connection parameters - adjust these to match your setup
    DB_CONFIG = {
        'host': '172.18.0.2',  # or your DB host
        'database': 'reviews',
        'user': 'airflow',
        'password': 'airflow',
        'port': 5432
    }
    
    # Create database connection
    connection_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    engine = create_engine(connection_string)
    
    try:
        # Read reviews from PostgreSQL
        # Adjust the table name and column names to match your schema
        
        
        # Try different possible column names for review text
        query = "SELECT id, review_text, rating FROM reviews_table WHERE sentiment_label IS NULL OR sentiment_label = ''"
        df = pd.read_sql(query, engine)
        review_column = 'review_text'
        if df is None or review_column is None:
            raise ValueError("Could not find reviews table or review text column")
        
        print(f"Found {len(df)} reviews to analyze using column '{review_column}'")
        
        if len(df) == 0:
            print("No new reviews to analyze")
            return
        
        # Initialize the French sentiment analysis pipeline
        print("Loading sentiment analysis model...")
        model_name = "nlptown/bert-base-multilingual-uncased-sentiment"
        
        # Load tokenizer to handle text truncation
        tokenizer = AutoTokenizer.from_pretrained(model_name)
        
        sentiment_pipeline = pipeline(
            "sentiment-analysis",
            model=model_name,
            tokenizer=tokenizer,
            return_all_scores=True,
            truncation=True,  # Enable truncation
            max_length=512    # Set maximum length
        )
        
        # Initialize new columns for sentiment analysis
        df['sentiment_label'] = ''
        df['sentiment_score'] = 0.0
        df['positive_score'] = 0.0
        df['negative_score'] = 0.0
        df['neutral_score'] = 0.0
        
        print(f"Processing {len(df)} reviews for sentiment analysis...")
        
        # Process reviews in batches to avoid memory issues
        batch_size = 50
        for i in range(0, len(df), batch_size):
            batch_end = min(i + batch_size, len(df))
            batch_reviews = df.iloc[i:batch_end][review_column].fillna('').tolist()
            
            # Filter out empty reviews and truncate long ones
            non_empty_reviews = []
            for idx, review in enumerate(batch_reviews):
                review_str = str(review).strip()
                if review_str:
                    # Truncate review to fit model's token limit
                    # We'll truncate to approximately 400 characters to be safe
                    # (roughly 100 tokens, well under the 512 limit)
                    if len(review_str) > 400:
                        review_str = review_str[:400] + "..."
                        print(f"Truncated long review at index {i + idx}")
                    non_empty_reviews.append((idx, review_str))
            
            if non_empty_reviews:
                # Extract just the review texts for processing
                review_texts = [review for _, review in non_empty_reviews]
                
                try:
                    # Perform sentiment analysis
                    results = sentiment_pipeline(review_texts)
                    
                    # Process results
                    for j, (original_idx, _) in enumerate(non_empty_reviews):
                        actual_idx = i + original_idx
                        result = results[j]
                        
                        # Extract scores for different sentiments
                        scores_dict = {item['label']: item['score'] for item in result}
                        
                        # Determine the dominant sentiment
                        best_sentiment = max(result, key=lambda x: x['score'])
                        
                        # Map the labels (may vary depending on the model)
                        sentiment_mapping = {
                            'POSITIVE': 'positive',
                            'NEGATIVE': 'negative',
                            'NEUTRAL': 'neutral',
                            '1 star': 'negative',
                            '2 stars': 'negative', 
                            '3 stars': 'neutral',
                            '4 stars': 'positive',
                            '5 stars': 'positive'
                        }
                        
                        mapped_sentiment = sentiment_mapping.get(best_sentiment['label'], best_sentiment['label'].lower())
                        
                        # Update dataframe
                        df.loc[actual_idx, 'sentiment_label'] = mapped_sentiment
                        df.loc[actual_idx, 'sentiment_score'] = best_sentiment['score']
                        
                        # Store individual scores if available
                        df.loc[actual_idx, 'positive_score'] = scores_dict.get('POSITIVE', scores_dict.get('4 stars', scores_dict.get('5 stars', 0)))
                        df.loc[actual_idx, 'negative_score'] = scores_dict.get('NEGATIVE', scores_dict.get('1 star', scores_dict.get('2 stars', 0)))
                        df.loc[actual_idx, 'neutral_score'] = scores_dict.get('NEUTRAL', scores_dict.get('3 stars', 0))
                    
                    print(f"Processed batch {i//batch_size + 1}/{(len(df) + batch_size - 1)//batch_size}")
                    
                except Exception as e:
                    print(f"Error processing batch starting at index {i}: {str(e)}")
                    # Set default values for failed batch
                    for j in range(batch_end - i):
                        df.loc[i + j, 'sentiment_label'] = 'unknown'
                        df.loc[i + j, 'sentiment_score'] = 0.0
        
        # Update PostgreSQL with sentiment analysis results
        print("Updating database with sentiment analysis results...")
        
        with engine.begin() as conn:  # this handles commit automatically
            for _, row in df.iterrows():
                update_query = """
                UPDATE reviews_table 
                SET sentiment_label = %s, 
                    sentiment_score = %s, 
                    positive_score = %s, 
                    negative_score = %s, 
                    neutral_score = %s
                WHERE id = %s
                """
                conn.execute(update_query, (
                    row['sentiment_label'],
                    float(row['sentiment_score']),
                    float(row['positive_score']),
                    float(row['negative_score']),
                    float(row['neutral_score']),
                    row['id']
                ))
        
        print(f"Sentiment analysis completed for {len(df)} reviews")
        print(f"Sentiment distribution:")
        print(df['sentiment_label'].value_counts())
        
    except Exception as e:
        print(f"Error in sentiment analysis: {str(e)}")
        raise
    finally:
        engine.dispose()
