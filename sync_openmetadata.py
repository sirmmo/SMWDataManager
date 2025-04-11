from settings import *
from mwclient import Site
import requests
import logging
from pathlib import Path
from typing import Dict, Optional, List
import datetime
import time
from metadata.generated.schema.entity.data.table import Table
from metadata.ingestion.ometa.openmetadata_api import OpenMetadataAPIClient
from metadata.generated.schema.security.client.openMetadataJWTClientConfig import OpenMetadataJWTClientConfig

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("openmetadata_sync.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("openmetadata_sync")

class OpenMetadataSynchronizer:
    def __init__(self, 
                 data_dir: str = "./data",
                 openmetadata_config: Dict = None):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        
        # OpenMetadata connection
        config = OpenMetadataJWTClientConfig(
            hostPort=openmetadata_config.get('host', 'http://localhost:8585/api'),
            jwtToken=openmetadata_config.get('jwt_token')
        )
        self.metadata = OpenMetadataAPIClient(config)
        
        # Wiki connection
        self.user_agent = 'OpenMetadataSync/1.0'
        self.site = Site(WIKI_BASE, 
                        force_login=False, 
                        scheme=PROTOCOL, 
                        path="/",
                        clients_useragent=self.user_agent, 
                        connection_options={"verify": False})
        self.site.clientlogin(username=USER, password=PASS)
        logger.info(f"Connected to wiki as {USER}")

    def get_tables(self) -> List[Table]:
        """Fetch all tables from OpenMetadata"""
        try:
            tables = self.metadata.list_entities(entity=Table)
            logger.info(f"Found {len(tables)} tables in OpenMetadata")
            return tables
        except Exception as e:
            logger.error(f"Failed to fetch tables from OpenMetadata: {str(e)}")
            return []

    def create_wiki_page(self, table: Table) -> str:
        """Create MediaWiki page content for a table"""
        PAGE = f"""
{{{{#knowledgegraph:
|nodes={{{{PAGENAME}}}}
|depth=10
|show-property-type=true
|graph-options=MediaWiki:KnowledgeGraphOptions
|property-options?Organization logo=KnowledgeGraphOptionsImage
|width=100%
|height=400px 
}}}}

[[Source::OpenMetadata]]
[[LastUpdated::{datetime.date.today().strftime("%Y-%m-%d")}]]

={table.name}=

== Info ==

{{{{DataSet
| name = {table.name}
| id = {table.id.__root__}
| description = {table.description or "No description available"}
| owner = {table.owner.name if table.owner else "Unknown"}
| service = {table.service.name if table.service else "Unknown"}
| database = {table.database.name if table.database else "Unknown"}
| schema = {table.databaseSchema.name if table.databaseSchema else "Unknown"}
}}}}

== Fields ==
"""
        # Process columns
        for column in table.columns:
            gb_type = self.determine_glassbox_type(column.dataType)
            
            PAGE += f"""{{{{DataSetColumn
|column={column.name}
|description={column.description or ""}
|type={column.dataType}
|GlassBoxType={gb_type}
}}}}\n"""

        # Add lineage information if available
        if hasattr(table, 'lineage') and table.lineage:
            PAGE += "\n== Lineage ==\n"
            for upstream in table.lineage.upstreamEdges or []:
                PAGE += f"* Upstream: {upstream.fromEntity.name}\n"
            for downstream in table.lineage.downstreamEdges or []:
                PAGE += f"* Downstream: {downstream.toEntity.name}\n"

        PAGE += """
== Logs ==
{{#ask:
[[RefersTo::{{PAGENAME}}]]
|?Timestamp
|?Result
}}
"""
        return PAGE

    def determine_glassbox_type(self, data_type: str) -> str:
        """Map OpenMetadata types to GlassBox types"""
        type_mapping = {
            'STRING': 'Anagrafica',
            'TEXT': 'Anagrafica',
            'CHAR': 'Anagrafica',
            'INTEGER': 'Metric',
            'BIGINT': 'Metric',
            'FLOAT': 'Metric',
            'DOUBLE': 'Metric',
            'DECIMAL': 'Metric',
            'TIMESTAMP': 'Timestamp',
            'DATE': 'Timestamp',
            'TIME': 'Timestamp',
            'GEOGRAPHY': 'Geographic',
            'GEOMETRY': 'Geographic'
        }
        return type_mapping.get(data_type.upper(), 'Unknown')

    def sync_table(self, table: Table) -> bool:
        """Synchronize a single table to the wiki"""
        try:
            title = f"OpenMetadata - {table.name}"
            page = self.site.pages[title]
            
            # Create or update page
            page_content = self.create_wiki_page(table)
            page.edit(page_content, summary='Synchronized from OpenMetadata')
            
            # Log success
            timestamp = int(time.time())
            log_page = self.site.pages[f"{title}/Log"]
            log_content = f"""{{{{Log
|refersto={title}
|timestamp={timestamp}
|result=true
}}}}
* '''Synchronized''': {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
"""
            log_page.edit(log_content, summary='Updated sync log')
            
            logger.info(f"Successfully synchronized {title}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to sync table {table.name}: {str(e)}")
            return False

    def sync_all(self):
        """Synchronize all tables from OpenMetadata"""
        total_synced = 0
        tables = self.get_tables()
        
        for table in tables:
            if self.sync_table(table):
                total_synced += 1
        
        logger.info(f"Sync complete. Successfully synchronized {total_synced} tables")

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Synchronize OpenMetadata with Wiki")
    parser.add_argument('--data-dir', default='./data', help='Data directory')
    parser.add_argument('--openmetadata-host', default='http://localhost:8585/api', 
                       help='OpenMetadata host URL')
    parser.add_argument('--jwt-token', required=True,
                       help='JWT token for OpenMetadata authentication')
    
    args = parser.parse_args()
    
    openmetadata_config = {
        'host': args.openmetadata_host,
        'jwt_token': args.jwt_token
    }
    
    synchronizer = OpenMetadataSynchronizer(
        data_dir=args.data_dir,
        openmetadata_config=openmetadata_config
    )
    synchronizer.sync_all()

if __name__ == "__main__":
    main()





