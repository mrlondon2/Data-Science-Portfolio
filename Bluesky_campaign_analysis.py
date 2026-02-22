import os
import json
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import pandas as pd
from dotenv import load_dotenv
import schedule

# Bluesky AT Protocol
from atproto import Client, models

# Sentiment Analysis
from textblob import TextBlob

# Google BigQuery
from google.cloud import bigquery
from google.oauth2 import service_account

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('campaign_analysis.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class BlueskyConfig:
    """Configuration management for Bluesky API and campaign settings"""
    
    def __init__(self):
        load_dotenv()
        
        self.bluesky_handle = os.getenv('BLUESKY_HANDLE')
        self.bluesky_password = os.getenv('BLUESKY_PASSWORD')
        
        self.gcp_project_id = os.getenv('GCP_PROJECT_ID')
        self.gcp_dataset_id = os.getenv('GCP_DATASET_ID', 'campaign_analytics')
        self.gcp_credentials_path = os.getenv('GCP_CREDENTIALS_PATH')
        
        self.randy_bryce_handle = 'ironstache.bsky.social'
        self.bryan_steil_handle = None
        
        self.keywords = [
            'randy bryce', 'randybryce', 
            'bryan steil', 'bryansteil',
            'wi01', 'wisconsin 1st district', 
            'congress', 'wi cd 1'
        ]
        
        self.hashtags = [
            '#RandyBryce', '#BryanSteil', '#WI01', 
            '#Wisconsin', '#Congress2026', '#Politics'
        ]
        
    def validate(self) -> bool:
        """Validate that all required configuration is present"""
        required = [
            self.bluesky_handle,
            self.bluesky_password,
            self.gcp_project_id
        ]
        return all(required)


class BlueskyDataCollector:
    """Handles data collection from Bluesky AT Protocol"""
    
    def __init__(self, config: BlueskyConfig):
        self.config = config
        self.client = None
        self.rate_limit_delay = 1  # in seconds
        
    def connect(self) -> bool:
        """Establish connection to Bluesky"""
        try:
            self.client = Client()
            self.client.login(
                self.config.bluesky_handle,
                self.config.bluesky_password
            )
            logger.info("Successfully connected to Bluesky")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Bluesky: {e}")
            return False
    
    def get_profile(self, handle: str) -> Optional[Dict[str, Any]]:
        """Get profile information for a handle"""
        try:
            time.sleep(self.rate_limit_delay)
            profile = self.client.get_profile(handle)
            
            return {
                'handle': handle,
                'display_name': profile.display_name,
                'followers_count': profile.followers_count,
                'follows_count': profile.follows_count,
                'posts_count': profile.posts_count,
                'description': profile.description,
                'timestamp': datetime.utcnow().isoformat()
            }
        except Exception as e:
            logger.error(f"Error fetching profile for {handle}: {e}")
            return None
    
    def get_author_feed(self, handle: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Get posts from a specific author"""
        try:
            time.sleep(self.rate_limit_delay)
            feed = self.client.get_author_feed(actor=handle, limit=limit)
            
            posts = []
            for item in feed.feed:
                post = item.post
                posts.append({
                    'uri': post.uri,
                    'cid': post.cid,
                    'author_handle': post.author.handle,
                    'author_display_name': post.author.display_name,
                    'text': post.record.text if hasattr(post.record, 'text') else '',
                    'created_at': post.record.created_at,
                    'like_count': post.like_count or 0,
                    'repost_count': post.repost_count or 0,
                    'reply_count': post.reply_count or 0,
                    'quote_count': getattr(post, 'quote_count', 0),
                    'langs': post.record.langs if hasattr(post.record, 'langs') else [],
                    'timestamp': datetime.utcnow().isoformat()
                })
            
            logger.info(f"Collected {len(posts)} posts from {handle}")
            return posts
            
        except Exception as e:
            logger.error(f"Error fetching feed for {handle}: {e}")
            return []
    
    def search_posts(self, query: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Search for posts containing specific keywords"""
        try:
            time.sleep(self.rate_limit_delay)
            results = self.client.app.bsky.feed.search_posts(
                params={'q': query, 'limit': limit}
            )
            
            posts = []
            for post in results.posts:
                posts.append({
                    'uri': post.uri,
                    'cid': post.cid,
                    'author_handle': post.author.handle,
                    'author_display_name': post.author.display_name,
                    'text': post.record.text if hasattr(post.record, 'text') else '',
                    'created_at': post.record.created_at,
                    'like_count': post.like_count or 0,
                    'repost_count': post.repost_count or 0,
                    'reply_count': post.reply_count or 0,
                    'quote_count': getattr(post, 'quote_count', 0),
                    'search_query': query,
                    'timestamp': datetime.utcnow().isoformat()
                })
            
            logger.info(f"Found {len(posts)} posts for query: {query}")
            return posts
            
        except Exception as e:
            logger.error(f"Error searching posts for '{query}': {e}")
            return []
    
    def collect_all_campaign_data(self) -> Dict[str, Any]:
        """Collect all campaign-related data"""
        data = {
            'profiles': [],
            'candidate_posts': [],
            'keyword_posts': [],
            'collection_time': datetime.utcnow().isoformat()
        }
        
        handles = [self.config.randy_bryce_handle]
        if self.config.bryan_steil_handle:
            handles.append(self.config.bryan_steil_handle)

        for handle in handles:
            profile = self.get_profile(handle)
            if profile:
                data['profiles'].append(profile)

        for handle in handles:
            posts = self.get_author_feed(handle, limit=50)
            data['candidate_posts'].extend(posts)
        
        for keyword in self.config.keywords[:5]:  # Limit to avoid rate limits
            posts = self.search_posts(keyword, limit=50)
            data['keyword_posts'].extend(posts)
        
        # Remove duplicates
        seen_uris = set()
        unique_posts = []
        for post in data['keyword_posts']:
            if post['uri'] not in seen_uris:
                seen_uris.add(post['uri'])
                unique_posts.append(post)
        data['keyword_posts'] = unique_posts
        
        logger.info(f"Total data collected: {len(data['profiles'])} profiles, "
                   f"{len(data['candidate_posts'])} candidate posts, "
                   f"{len(data['keyword_posts'])} keyword posts")
        
        return data


class SentimentAnalyzer:
    """Analyzes sentiment and extracts insights from text"""
    
    @staticmethod
    def analyze_sentiment(text: str) -> Dict[str, float]:
        """Analyze sentiment of text using TextBlob"""
        try:
            blob = TextBlob(text)
            polarity = blob.sentiment.polarity  # -1 to 1
            subjectivity = blob.sentiment.subjectivity  # 0 to 1
            
            # Categorize sentiment
            if polarity > 0.1:
                category = 'positive'
            elif polarity < -0.1:
                category = 'negative'
            else:
                category = 'neutral'
            
            return {
                'polarity': polarity,
                'subjectivity': subjectivity,
                'category': category
            }
        except Exception as e:
            logger.error(f"Error analyzing sentiment: {e}")
            return {
                'polarity': 0.0,
                'subjectivity': 0.0,
                'category': 'neutral'
            }
    
    @staticmethod
    def extract_hashtags(text: str) -> List[str]:
        """Extract hashtags from text"""
        words = text.split()
        hashtags = [word for word in words if word.startswith('#')]
        return hashtags
    
    @staticmethod
    def extract_mentions(text: str) -> List[str]:
        """Extract @mentions from text"""
        words = text.split()
        mentions = [word for word in words if word.startswith('@')]
        return mentions


class DataProcessor:
    """Process and transform collected data"""
    
    def __init__(self, config: BlueskyConfig):
        self.config = config
        self.analyzer = SentimentAnalyzer()
    
    def process_posts(self, posts: List[Dict[str, Any]]) -> pd.DataFrame:
        """Process posts and add analytics"""
        if not posts:
            return pd.DataFrame()
        
        df = pd.DataFrame(posts)
        
        sentiments = df['text'].apply(self.analyzer.analyze_sentiment)
        df['sentiment_polarity'] = sentiments.apply(lambda x: x['polarity'])
        df['sentiment_subjectivity'] = sentiments.apply(lambda x: x['subjectivity'])
        df['sentiment_category'] = sentiments.apply(lambda x: x['category'])
        
        df['hashtags'] = df['text'].apply(self.analyzer.extract_hashtags)
        df['mentions'] = df['text'].apply(self.analyzer.extract_mentions)
        df['hashtag_count'] = df['hashtags'].apply(len)
        df['mention_count'] = df['mentions'].apply(len)
        
        df['total_engagement'] = df['like_count'] + df['repost_count'] + df['reply_count']
        
        df['mentions_bryce'] = df['text'].str.lower().str.contains('randy bryce|randybryce|@randybryce')
        df['mentions_steil'] = df['text'].str.lower().str.contains('bryan steil|bryansteil|@bryansteil')
        
        df['created_at'] = pd.to_datetime(df['created_at'], format='mixed', utc=True)
        df['created_at'] = df['created_at'].dt.floor('us')  # Round to microseconds for BigQuery compatibility
        df['timestamp'] = pd.to_datetime(df['timestamp']).dt.floor('us')
        df['date'] = df['created_at'].dt.date
        df['hour'] = df['created_at'].dt.hour
        df['day_of_week'] = df['created_at'].dt.day_name()
        
        return df
    
    def create_daily_aggregates(self, posts_df: pd.DataFrame) -> pd.DataFrame:
        """Create daily aggregate metrics"""
        if posts_df.empty:
            return pd.DataFrame()
        
        daily = posts_df.groupby('date').agg({
            'uri': 'count',
            'like_count': 'sum',
            'repost_count': 'sum',
            'reply_count': 'sum',
            'total_engagement': 'sum',
            'sentiment_polarity': 'mean',
            'mentions_bryce': 'sum',
            'mentions_steil': 'sum'
        }).reset_index()
        
        daily.columns = [
            'date', 'post_count', 'total_likes', 'total_reposts',
            'total_replies', 'total_engagement', 'avg_sentiment',
            'bryce_mentions', 'steil_mentions'
        ]
        
        return daily
    
    def create_comparative_metrics(self, posts_df: pd.DataFrame, profiles: List[Dict]) -> pd.DataFrame:
        """Create head-to-head comparison metrics"""
        metrics = []
        
        for profile in profiles:
            handle = profile['handle']
            candidate_posts = posts_df[posts_df['author_handle'] == handle]
            
            if not candidate_posts.empty:
                metrics.append({
                    'candidate': 'Randy Bryce' if 'randybryce' in handle.lower() else 'Bryan Steil',
                    'handle': handle,
                    'followers': profile.get('followers_count', 0),
                    'total_posts': len(candidate_posts),
                    'avg_likes': candidate_posts['like_count'].mean(),
                    'avg_reposts': candidate_posts['repost_count'].mean(),
                    'avg_replies': candidate_posts['reply_count'].mean(),
                    'avg_engagement': candidate_posts['total_engagement'].mean(),
                    'avg_sentiment': candidate_posts['sentiment_polarity'].mean(),
                    'positive_posts': len(candidate_posts[candidate_posts['sentiment_category'] == 'positive']),
                    'negative_posts': len(candidate_posts[candidate_posts['sentiment_category'] == 'negative']),
                    'neutral_posts': len(candidate_posts[candidate_posts['sentiment_category'] == 'neutral']),
                    'timestamp': datetime.utcnow().isoformat()
                })
        
        # Add share of voice metrics
        bryce_mentions = posts_df['mentions_bryce'].sum()
        steil_mentions = posts_df['mentions_steil'].sum()
        total_mentions = bryce_mentions + steil_mentions
        
        if total_mentions > 0:
            for metric in metrics:
                if 'Bryce' in metric['candidate']:
                    metric['share_of_voice'] = (bryce_mentions / total_mentions) * 100
                else:
                    metric['share_of_voice'] = (steil_mentions / total_mentions) * 100
        
        return pd.DataFrame(metrics)


class BigQueryExporter:
    """Export data to Google BigQuery for Looker Studio"""
    
    def __init__(self, config: BlueskyConfig):
        self.config = config
        self.client = None
        
    def connect(self) -> bool:
        """Connect to BigQuery"""
        try:
            if self.config.gcp_credentials_path:
                credentials = service_account.Credentials.from_service_account_file(
                    self.config.gcp_credentials_path
                )
                self.client = bigquery.Client(
                    credentials=credentials,
                    project=self.config.gcp_project_id
                )
            else:
                self.client = bigquery.Client(project=self.config.gcp_project_id)
            
            logger.info("Successfully connected to BigQuery")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to BigQuery: {e}")
            return False
    
    def create_dataset_if_not_exists(self):
        """Create dataset if it doesn't exist"""
        try:
            dataset_id = f"{self.config.gcp_project_id}.{self.config.gcp_dataset_id}"
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = "US"
            
            self.client.create_dataset(dataset, exists_ok=True)
            logger.info(f"Dataset {dataset_id} ready")
        except Exception as e:
            logger.error(f"Error creating dataset: {e}")
    
    def export_dataframe(self, df: pd.DataFrame, table_name: str, 
                        write_disposition: str = 'WRITE_APPEND') -> bool:
        """Export DataFrame to BigQuery table"""
        try:
            if df.empty:
                logger.warning(f"Empty DataFrame, skipping export to {table_name}")
                return False
            
            table_id = f"{self.config.gcp_project_id}.{self.config.gcp_dataset_id}.{table_name}"
            
            job_config = bigquery.LoadJobConfig(
                write_disposition=write_disposition,
                autodetect=True
            )
            
            job = self.client.load_table_from_dataframe(
                df, table_id, job_config=job_config
            )
            job.result()  # Wait for completion
            
            logger.info(f"Exported {len(df)} rows to {table_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error exporting to BigQuery table {table_name}: {e}")
            return False
    
    def export_all_data(self, processed_data: Dict[str, pd.DataFrame]) -> bool:
        """Export all processed data to BigQuery"""
        success = True
        
        if 'posts' in processed_data and not processed_data['posts'].empty:
            success &= self.export_dataframe(
                processed_data['posts'],
                'posts',
                'WRITE_APPEND'
            )
        
        if 'daily_aggregates' in processed_data and not processed_data['daily_aggregates'].empty:
            success &= self.export_dataframe(
                processed_data['daily_aggregates'],
                'daily_aggregates',
                'WRITE_TRUNCATE'  # Replace for daily summaries
            )
        
        if 'comparative_metrics' in processed_data and not processed_data['comparative_metrics'].empty:
            success &= self.export_dataframe(
                processed_data['comparative_metrics'],
                'comparative_metrics',
                'WRITE_APPEND'
            )
        
        if 'profiles' in processed_data and not processed_data['profiles'].empty:
            success &= self.export_dataframe(
                processed_data['profiles'],
                'candidate_profiles',
                'WRITE_TRUNCATE'  # Replace for current stats
            )
        
        return success


class CampaignAnalysisTool:
    """Main orchestrator for campaign analysis"""
    
    def __init__(self):
        self.config = BlueskyConfig()
        self.collector = BlueskyDataCollector(self.config)
        self.processor = DataProcessor(self.config)
        self.exporter = BigQueryExporter(self.config)
        
    def initialize(self) -> bool:
        """Initialize all components"""
        if not self.config.validate():
            logger.error("Configuration validation failed. Check environment variables.")
            return False
        
        if not self.collector.connect():
            return False
        
        if not self.exporter.connect():
            logger.warning("BigQuery connection failed. Data will not be exported.")
        else:
            self.exporter.create_dataset_if_not_exists()
        
        return True
    
    def run_analysis(self) -> Dict[str, pd.DataFrame]:
        """Run complete analysis pipeline"""
        logger.info("Starting campaign analysis...")
        
        # Collect data
        raw_data = self.collector.collect_all_campaign_data()
        
        # Process data
        all_posts = raw_data['candidate_posts'] + raw_data['keyword_posts']
        posts_df = self.processor.process_posts(all_posts)
        
        daily_aggregates = self.processor.create_daily_aggregates(posts_df)
        comparative_metrics = self.processor.create_comparative_metrics(
            posts_df, 
            raw_data['profiles']
        )
        profiles_df = pd.DataFrame(raw_data['profiles'])
        
        processed_data = {
            'posts': posts_df,
            'daily_aggregates': daily_aggregates,
            'comparative_metrics': comparative_metrics,
            'profiles': profiles_df
        }
        
        # Export to BigQuery
        self.exporter.export_all_data(processed_data)
        
        self.save_local_exports(processed_data)
        
        logger.info("Analysis complete!")
        return processed_data
    
    def save_local_exports(self, data: Dict[str, pd.DataFrame]):
        """Save CSV exports locally"""
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        
        for name, df in data.items():
            if not df.empty:
                filename = f"export_{name}_{timestamp}.csv"
                df.to_csv(filename, index=False)
                logger.info(f"Saved local export: {filename}")
    
    def print_summary(self, data: Dict[str, pd.DataFrame]):
        """Print summary statistics"""
        print("\n" + "="*60)
        print("CAMPAIGN ANALYSIS SUMMARY")
        print("="*60)
        
        if not data['comparative_metrics'].empty:
            print("\nCANDIDATE COMPARISON:")
            print(data['comparative_metrics'][['candidate', 'followers', 'avg_engagement', 
                                               'avg_sentiment', 'share_of_voice']].to_string(index=False))
        
        if not data['posts'].empty:
            print(f"\nTOTAL POSTS ANALYZED: {len(data['posts'])}")
            print(f"Date Range: {data['posts']['created_at'].min()} to {data['posts']['created_at'].max()}")
            
            sentiment_counts = data['posts']['sentiment_category'].value_counts()
            print(f"\nSENTIMENT BREAKDOWN:")
            for sentiment, count in sentiment_counts.items():
                print(f"  {sentiment.capitalize()}: {count} ({count/len(data['posts'])*100:.1f}%)")
        
        print("\n" + "="*60 + "\n")


def scheduled_job():
    """Job to run on schedule"""
    logger.info("Running scheduled analysis job...")
    tool = CampaignAnalysisTool()
    if tool.initialize():
        data = tool.run_analysis()
        tool.print_summary(data)


def main():
    """Main entry point"""
    tool = CampaignAnalysisTool()
    
    if not tool.initialize():
        logger.error("Failed to initialize campaign analysis tool")
        return
    
    # Run immediate analysis
    logger.info("Running initial analysis...")
    data = tool.run_analysis()
    tool.print_summary(data)
    
    # Scheduled Runs
    # Hourly automated collection
    # schedule.every(1).hours.do(scheduled_job)

    # logger.info("Scheduler started. Running hourly analysis...")
    # while True:
    #     schedule.run_pending()
    #     time.sleep(60)


if __name__ == "__main__":
    main()
