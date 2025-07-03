# Semantic MediaWiki for Extended GlassBox Urban Digital Twin

A semantic MediaWiki installation configured for the Extended GlassBox Urban Digital Twin project, providing the **Semantic Ingestion Layer** for dataset description and management.

## Overview

This MediaWiki instance serves as the core semantic data description platform for the Extended GlassBox model, enabling non-technical operators to create and manage semantic descriptions of urban datasets using enhanced templates.

### Purpose

- **Semantic Data Description**: Create semantic metadata for urban datasets
- **Template-based Data Modeling**: Use predefined templates for GlassBox model elements
- **Non-technical Interface**: Allow domain experts to describe data without coding
- **Ontology Management**: Support emerging urban ontology development
- **Rule Definition**: Define simulation rules through semantic forms

## Prerequisites

- MediaWiki 1.39+
- PHP 8.0+
- MySQL 8.0+ or PostgreSQL 13+
- Semantic MediaWiki extension 4.0+
- Semantic Forms extension (optional but recommended)

## Installation

### 1. Basic MediaWiki Setup

```bash
# Download MediaWiki
wget https://releases.wikimedia.org/mediawiki/1.39/mediawiki-1.39.6.tar.gz
tar -xzf mediawiki-1.39.6.tar.gz
mv mediawiki-1.39.6 /var/www/html/wiki
```

### 2. Database Configuration

```sql
CREATE DATABASE glassbox_wiki CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER 'wiki_user'@'localhost' IDENTIFIED BY 'secure_password';
GRANT ALL PRIVILEGES ON glassbox_wiki.* TO 'wiki_user'@'localhost';
FLUSH PRIVILEGES;
```

### 3. MediaWiki Installation

Access `http://your-domain/wiki` and complete the installation wizard with these settings:

- **Database type**: MySQL/PostgreSQL
- **Database name**: `glassbox_wiki`
- **Database username**: `wiki_user`
- **Wiki name**: `GlassBox Urban Digital Twin`

### 4. Install Semantic MediaWiki

```bash
cd /var/www/html/wiki
composer require mediawiki/semantic-media-wiki "~4.0"
```

Add to `LocalSettings.php`:
```php
<?php
// Enable Semantic MediaWiki
wfLoadExtension( 'SemanticMediaWiki' );
enableSemantics( 'your-domain.com/wiki' );

// Additional semantic extensions
wfLoadExtension( 'SemanticForms' );
wfLoadExtension( 'SemanticResultFormats' );

// GlassBox specific configuration
$smwgNamespacesWithSemanticLinks[NS_MAIN] = true;
$smwgQMaxSize = 5000;
$smwgQMaxDepth = 10;
```

### 5. Initialize Semantic Database

```bash
php maintenance/runJobs.php
php extensions/SemanticMediaWiki/maintenance/setupStore.php
```

#### Query Performance Issues
```php
# Increase query limits in LocalSettings.php
$smwgQMaxSize = 10000;
$smwgQMaxDepth = 15;
```

### Performance Optimization

```sql
-- Add indexes for semantic queries
CREATE INDEX idx_smw_title ON smw_object_ids (smw_title);
CREATE INDEX idx_smw_namespace ON smw_object_ids (smw_namespace);
CREATE INDEX idx_smw_iw ON smw_object_ids (smw_iw);
```

## Contributing

### Adding New Templates
1. Create template page with semantic markup
2. Define required semantic properties
3. Add to appropriate categories
4. Create corresponding form if needed
5. Update documentation

### Template Guidelines
- Use consistent naming conventions
- Include comprehensive documentation
- Add appropriate categories
- Define clear semantic properties
- Provide usage examples

## Support

For semantic MediaWiki specific issues:
- Check MediaWiki logs: `/var/log/mediawiki/`
- Enable debug mode in `LocalSettings.php`
- Consult Semantic MediaWiki documentation
- Review template syntax and property definitions
