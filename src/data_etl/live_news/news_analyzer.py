import requests
from datetime import datetime
import time
from groq import Groq
from elasticsearch import Elasticsearch
import json

class NewsAnalyzer:
    def __init__(self, session):
        settings = get_settings()
        self.es = Elasticsearch(
            [settings.ELASTICSEARCH_HOST],
            request_timeout=30,
            verify_certs=False,
            headers={"Content-Type": "application/json"},
            api_key=None
        )
        self.api_url = settings.NEWS_API_URL
        self.groq_client = Groq(api_key=settings.GROQ_API_KEY)
        self.geocode_api_key = settings.OPENCAGE_API_KEY
        self.geocode_url = settings.OPENCAGE_GEOCODE_URL
        
        self.create_index()
        
    def create_index(self):
        news_mapping = {
            "mappings": {
                "properties": {
                    "uri": {"type": "keyword"},
                    "lang": {"type": "keyword"},
                    "isDuplicate": {"type": "boolean"},
                    "date": {"type": "date"},
                    "dateTimePub": {"type": "date"},
                    "dataType": {"type": "keyword"},
                    "sim": {"type": "float"},
                    "url": {"type": "keyword"},
                    "title": {"type": "text"},
                    "body": {"type": "text"},
                    "source": {
                        "type": "object",
                        "properties": {
                            "uri": {"type": "keyword"},
                            "dataType": {"type": "keyword"},
                            "title": {"type": "keyword"}
                        }
                    }              
                }
            }
        }
        
        if self.es.indices.exists(index='news_events'):
            self.es.indices.delete(index='news_events')
        
        self.es.indices.create(index='news_events', body=news_mapping)

    def fetch_news(self, page=1):
        try:
            url = self.api_url
            print(f"Trying to fetch from: {url}")
            
            params = {
                "action": "getArticles",
                "keyword": "terror attack",
                "ignoreSourceGroupUri": "paywall/paywalled_sources",
                "articlesPage": page,
                "articlesCount": 100,
                "articlesSortBy": "socialScore",
                "articlesSortByAsc": "false",
                "dataType": "news,pr",
                "forceMaxDataTimeWindow": 31,
                "resultType": "articles"
            }
            
            print(f"Sending params: {json.dumps(params, indent=2)}")
            response = requests.get(url, params=params)
            print(f"Response status: {response.status_code}")
            print(f"Response headers: {response.headers}")
            
            if response.status_code != 200:
                print(f"Error response: {response.text}")
                return []
            
            data = response.json()
            print(f"Response data structure: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
            
            if 'articles' in data and 'results' in data['articles']:
                articles = data['articles']['results']
                print(f"Found {len(articles)} articles")
                return [{
                    "uri": article.get('uri', ''),
                    "lang": article.get('lang', ''),
                    "isDuplicate": article.get('isDuplicate', False),
                    "date": article.get('dateTime', ''),
                    "dateTimePub": article.get('dateTimePub', ''),
                    "dataType": article.get('dataType', ''),
                    "sim": 0,
                    "url": article.get('url', ''),
                    "title": article.get('title', ''),
                    "body": article.get('body', ''),
                    "source": {
                        "uri": article.get('source', {}).get('uri', ''),
                        "dataType": article.get('source', {}).get('dataType', ''),
                        "title": article.get('source', {}).get('title', '')
                    }
                } for article in articles]
                
            return []
        except Exception as e:
            print(f"Error fetching news: {e}")
            print(f"Full error: {str(e)}")
            return []

    def classify_news(self, text):
        '''groq nlp classification'''
        prompt = f"""Classify the following news text into one of these categories:
        1. General News
        2. Historical Terror Event
        3. Current Terror Event
        
        Text: {text}
        
        Return ONLY the number (1, 2, or 3) without any additional text."""
        
        response = self.groq_client.chat.completions.create(
            messages=[{"role": "user", "content": prompt}],
            model="mixtral-8x7b-32768"
        )
        
        try:
            result = response.choices[0].message.content.strip()
            number = ''.join(filter(str.isdigit, result))
            return int(number)
        except Exception as e:
            print(f"Error parsing classification result: {result}")
            return 1  # default to general news

    def extract_location(self, text):
        """Extract location from text using Groq and OpenCage Geocoding API"""
        try:
            '''groq nlp location extraction'''
            prompt = f"""Extract the location (city, country, region) from this text:
            {text}
            Return only the most relevant location name."""

            response = self.groq_client.chat.completions.create(
                messages=[{"role": "user", "content": prompt}],
                model="mixtral-8x7b-32768"
            )
            location = response.choices[0].message.content.strip()
            
            geocode_url = self.geocode_url
            params = {
                'q': location,
                'key': self.geocode_api_key,
                'pretty': 1,
                'no_annotations': 0
            }
            
            response = requests.get(geocode_url, params=params)
            data = response.json()
            
            if data['results']:
                result = data['results'][0]
                return {
                    "bounds": result.get('bounds', {
                        "northeast": {"lat": 0, "lng": 0},
                        "southwest": {"lat": 0, "lng": 0}
                    }),
                    "components": {
                        "ISO_3166-1_alpha-2": result.get('components', {}).get('ISO_3166-1_alpha-2', ''),
                        "ISO_3166-1_alpha-3": result.get('components', {}).get('ISO_3166-1_alpha-3', ''),
                        "ISO_3166-2": result.get('components', {}).get('ISO_3166-2', []),
                        "_category": result.get('components', {}).get('_category', ''),
                        "_normalized_city": result.get('components', {}).get('city', ''),
                        "_type": result.get('components', {}).get('_type', ''),
                        "city": result.get('components', {}).get('city', ''),
                        "continent": result.get('components', {}).get('continent', ''),
                        "country": result.get('components', {}).get('country', '')
                    },
                    "confidence": result.get('confidence', 0),
                    "formatted": result.get('formatted', ''),
                    "geometry": {
                        "lat": result.get('geometry', {}).get('lat', 0),
                        "lng": result.get('geometry', {}).get('lng', 0)
                    }
                }
                
            return None
            
        except Exception as e:
            print(f"Location extraction error: {e}")
            return None

    def process_article(self, article):
        '''Process article data for storage in Elasticsearch'''
        try:
            date = article.get('dateTime', None)
            date_pub = article.get('dateTimePub', None)
            
            if not date:
                date = datetime.now().isoformat()
            if not date_pub:
                date_pub = datetime.now().isoformat()
            
            return {
                "uri": article.get('uri', ''),
                "lang": article.get('lang', ''),
                "isDuplicate": article.get('isDuplicate', False),
                "date": date,
                "dateTimePub": date_pub,
                "dataType": article.get('dataType', ''),
                "sim": article.get('sim', 0),
                "url": article.get('url', ''),
                "title": article.get('title', ''),
                "body": article.get('body', ''),
                "source": {
                    "uri": article.get('source', {}).get('uri', ''),
                    "dataType": article.get('source', {}).get('dataType', ''),
                    "title": article.get('source', {}).get('title', '')
                }
            }
        except Exception as e:
            print(f"Error processing article: {e}")
            return None

    def analyze_and_store(self):
        total_processed = 0
        page = 1
        
        try:
            articles = self.fetch_news(page)
            print(f"Found {len(articles)} articles to process")
            
            for article in articles:
                processed_article = self.process_article(article)
                if processed_article:
                    self.es.index(index='news_events', body=processed_article)
                    total_processed += 1
                
            return total_processed
            
        except Exception as e:
            print(f"Error in analyze_and_store: {e}")
            return total_processed

    def get_news_analysis(self):
        '''Analyze stored news articles for terror'''
        try:
            query = {
                "size": 10,
                "_source": ["title", "body", "dateTimePub", "source.title"]
            }
            
            print("Fetching articles from Elasticsearch...")
            results = self.es.search(index='news_events', body=query)
            terror_events = []
            total_articles = len(results['hits']['hits'])
            
            print(f"\nAnalyzing {total_articles} articles...")
            for i, hit in enumerate(results['hits']['hits'], 1):
                article = hit['_source']
                print(f"\nProcessing article {i}/{total_articles}: {article['title'][:50]}...")
                
                print("Classifying article...")
                news_type = self.classify_news(article['body'])
                print(f"Classification result: {news_type}")
                
                if news_type in [2, 3]:
                    print("Terror event detected, extracting location...")
                    location_data = self.extract_location(article['body'])
                    if location_data:
                        print(f"Location found: {location_data.get('formatted', 'Unknown')}")
                        terror_events.append(location_data)
                    else:
                        print("No location data found")
            
            print(f"\nFound {len(terror_events)} terror events with location data")
            return terror_events
            
        except Exception as e:
            print(f"Error analyzing news: {e}")
            return []

def run_news_analyzer(session):
    analyzer = NewsAnalyzer(session)
    
    while True:
        try:
            print(f"\nAnalyzing news at {datetime.now()}")
            new_articles = analyzer.analyze_and_store()
            print(f"Processed {new_articles} new articles")
            
            time.sleep(120)
            
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(30)

if __name__ == "__main__":
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from src.config.settings import get_settings
    import time
    
    settings = get_settings()
    engine = create_engine(settings.POSTGRES_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    analyzer = NewsAnalyzer(session)
    
    print("Storing initial news articles...")
    total_stored = analyzer.analyze_and_store()
    print(f"Stored {total_stored} articles")
    
    time.sleep(1)
    
    print("\nGetting news analysis...")
    terror_events = analyzer.get_news_analysis()
    
    if terror_events:
        print("\nTerror Events Locations:")
        print(json.dumps(terror_events, indent=2, ensure_ascii=False))
    else:
        print("\nNo terror events found in the analyzed articles")
