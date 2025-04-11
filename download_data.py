#!/usr/bin/env python3
import os
import mwclient
import pandas as pd
import geopandas as gpd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import logging
from typing import Dict, List, Optional
from dataclasses import dataclass
from pathlib import Path
import time
import datetime

from settings import * 

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("geodata_processing.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("geodata_processing")

@dataclass
class DatasetJoin:
    left_dataset: str
    right_dataset: str
    left_column: str
    right_column: str

class GeoDataProcessor:
    def __init__(self, wiki_url: str, username: str, password: str, data_dir: str):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Setup MediaWiki connection
        parsed_url = requests.utils.urlparse(wiki_url)
        self.site = mwclient.Site(
            parsed_url.netloc,
            path=parsed_url.path,
            scheme=parsed_url.scheme
        )
        self.site.login(username, password)
        logger.info(f"Connected to MediaWiki at {wiki_url}")

    def get_dataset_config(self) -> List[Dict]:
        """Fetch dataset configurations from SMW"""
        query = '[[IsA::DataSet]]|?DirectLink|?HasFormat|?Name'
        results = self.site.ask(query)
        ret = list(results)
        logger.info(f"Found {len(ret)} datasets")
        return ret

    def get_join_config(self) -> List[DatasetJoin]:
        """Fetch join configurations from SMW"""
        query = '[[IsA::DataSetJoin]]|?LeftDataSet|?LeftColumn|?RightDataSet|?RightColumn'
        results = self.site.ask(query)
        
        joins = []
        for result in results:
            joins.append(DatasetJoin(
                left_dataset=result['printouts']['LeftDataSet'][0],
                right_dataset=result['printouts']['RightDataSet'][0],
                left_column=result['printouts']['LeftColumn'][0],
                right_column=result['printouts']['RightColumn'][0]
            ))
        
        logger.info(f"Found {len(joins)} join configurations")
        return joins

    def log_to_wiki(self, dataset_name: str, success: bool, error: str = None) -> bool:
        """
        Log the download result to the wiki using the Log template.
        
        Args:
            dataset_name: Name of the dataset
            success: Whether the download was successful
            error: Error message if any
        
        Returns:
            bool: Whether the logging was successful
        """
        try:
            if not self.site.logged_in:
                logger.error("Not logged in to MediaWiki")
                return False
            
            # Create timestamp
            timestamp = int(time.time())
            formatted_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Create page name with timestamp
            page_name = f"{dataset_name} @ {formatted_time}"
            
            # Create log entry using the template
            log_entry = (
                f"{{{{Log|refersto={dataset_name}|timestamp={timestamp}|"
                f"result={'true' if success else 'false'}}}}}"
            )
            
            if not success and error:
                log_entry += f"\n* '''Error''': {error}"
            
            # Create or edit the page
            page = self.site.pages[page_name]
            page.edit(log_entry, summary=f"Logged dataset download result for {dataset_name}")
            
            logger.info(f"Successfully logged result for {dataset_name} to wiki")
            return True
            
        except Exception as e:
            logger.error(f"Failed to log to wiki: {str(e)}")
            return False

    def download_dataset(self, dataset_info: Dict) -> Optional[Path]:
        """Download a single dataset"""
        dataset_name = dataset_info['fulltext']
        try:
            direct_links = dataset_info['printouts'].get('DirectLink', [])
            if not direct_links:
                error_msg = f"No download URL for {dataset_name}"
                logger.error(error_msg)
                self.log_to_wiki(dataset_name, False, error_msg)
                return None

            url = direct_links[0]
            output_path = self.data_dir / f"{dataset_info['fulltext']}.parquet"
            
            response = requests.get(url)
            response.raise_for_status()

            # Get format from HasFormat property
            data_formats = dataset_info['printouts'].get('HasFormat', [])
            data_format = data_formats[0]['fulltext'] if data_formats else None

            # Determine format and load data based on HasFormat property
            if data_format and 'geojson' in data_format.lower():
                df = gpd.read_file(response.text)
            elif data_format and 'csv' in data_format.lower():
                df = pd.read_csv(StringIO(response.text))
            elif data_format and 'json' in data_format.lower():
                df = pd.read_json(response.text)
            elif data_format and 'zip' in data_format.lower():
                # Save zip file temporarily
                temp_zip = self.data_dir / "temp.zip"
                with open(temp_zip, 'wb') as f:
                    f.write(response.content)
                # Read shapefile from zip
                df = gpd.read_file(f"zip://{temp_zip}")
                temp_zip.unlink()  # Clean up temp file
            elif data_format and 'parquet' in data_format.lower():
                # Save parquet file temporarily
                temp_parquet = self.data_dir / "temp.parquet"
                with open(temp_parquet, 'wb') as f:
                    f.write(response.content)
                df = pd.read_parquet(temp_parquet)
                temp_parquet.unlink()  # Clean up temp file
            else:
                logger.error(f"Unsupported format {data_format} for {url}")
                return None

            # Save as GeoParquet
            if isinstance(df, gpd.GeoDataFrame):
                df.to_parquet(output_path)
            else:
                # Convert to GeoParquet if it has lat/lon columns
                if {'latitude', 'longitude'}.issubset(df.columns):
                    gdf = gpd.GeoDataFrame(
                        df,
                        geometry=gpd.points_from_xy(df.longitude, df.latitude),
                        crs="EPSG:4326"
                    )
                    gdf.to_parquet(output_path)
                else:
                    df.to_parquet(output_path)

            logger.info(f"Downloaded and converted {dataset_name}")
            self.log_to_wiki(dataset_name, True)
            return output_path

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error processing {dataset_name}: {error_msg}")
            self.log_to_wiki(dataset_name, False, error_msg)
            return None

    def process_join(self, join: DatasetJoin) -> Optional[Path]:
        """Process a single join configuration"""
        try:
            left_path = self.data_dir / f"{join.left_dataset}.parquet"
            right_path = self.data_dir / f"{join.right_dataset}.parquet"

            if not (left_path.exists() and right_path.exists()):
                logger.error(f"Missing files for join: {join}, {left_path}, {right_path}")
                return None

            # Read datasets
            left_df = pd.read_parquet(left_path)
            right_df = pd.read_parquet(right_path)

            # Perform join
            merged_df = left_df.merge(
                right_df,
                left_on=join.left_column,
                right_on=join.right_column,
                how='inner'
            )

            # Save joined result
            output_path = self.data_dir / f"{join.left_dataset}_{join.right_dataset}_joined.parquet"
            
            if isinstance(merged_df, gpd.GeoDataFrame):
                merged_df.to_parquet(output_path)
            else:
                merged_df.to_parquet(output_path)

            logger.info(f"Processed join: {join.left_dataset} + {join.right_dataset}")
            return output_path

        except Exception as e:
            logger.error(f"Error processing join: {str(e)}")
            return None

    def process_all(self):
        """Process all datasets and joins"""
        # Get configurations
        datasets = self.get_dataset_config()
        joins = self.get_join_config()

        # Download all datasets
        for dataset in datasets:
            print('dling', dataset)
            self.download_dataset(dataset)

        # Process all joins
        for join in joins:
            self.process_join(join)

def main():
    import argparse
    import os
    parser = argparse.ArgumentParser(description="Process geodata from SMW")
    parser.add_argument('--wiki-url', default = PROTOCOL+"://"+WIKI_BASE+"/", help='MediaWiki URL')
    parser.add_argument('--username', default=USERNAME, help='MediaWiki username')
    parser.add_argument('--password', default=PASSWORD, help='MediaWiki password')
    parser.add_argument('--data-dir', default='./geodata', help='Output directory')
    
    args = parser.parse_args()
    
    processor = GeoDataProcessor(
        wiki_url=args.wiki_url,
        username=args.username,
        password=args.password,
        data_dir=args.data_dir
    )
    
    processor.process_all()

if __name__ == "__main__":
    main()





