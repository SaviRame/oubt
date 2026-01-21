# Day 17: Athena & Serverless Analytics - Hands-on Materials

This directory contains the hands-on materials for Day 17 of the training program, focusing on Athena & Serverless Analytics.

## Files Included

1. **[athena-serverless-analytics.md](athena-serverless-analytics.md)** - Main hands-on plan document
   - Comprehensive guide covering all technical concepts
   - Step-by-step implementation instructions
   - Governance strategies and best practices
   - QuickSight dashboard design with governance indicators

2. **[athena-external-tables.sql](athena-external-tables.sql)** - SQL script for Athena external tables
   - Creates external tables for NYC taxi data
   - Implements partitioning strategy
   - Sets up certified views for self-service analytics
   - Includes governance metadata views

3. **[setup-athena-workgroup.py](setup-athena-workgroup.py)** - Python script for Athena workgroup setup
   - Creates Athena workgroup with usage controls
   - Sets up CloudWatch dashboard for monitoring
   - Configures alarms for query governance
   - Implements cost management controls

4. **[redshift-scd2-setup.sql](redshift-scd2-setup.sql)** - SQL script for enhancing existing Redshift dimensional model
   - Enhances existing Day 16 tables with SCD Type 2 capabilities
   - Adds date dimension and foreign keys to fact table
   - Creates procedures for date dimension maintenance
   - Creates analytics views with governance indicators

## Prerequisites

Before starting the hands-on exercises, ensure you have:

1. **AWS Account** with appropriate permissions
2. **S3 Bucket** for storing data and query results
3. **IAM Roles** with necessary permissions for Athena, Redshift, and QuickSight
4. **NYC Taxi Data** in S3 (yellow taxi trip records and zone lookup data)
5. **Redshift Cluster** provisioned and accessible with existing Day 16 tables:
   - `mdm.zone_dim`
   - `mdm.vendor_dim`
   - `analytics.fact_taxi_trips`
6. **QuickSight Account** with appropriate permissions

## Implementation Sequence

### Phase 1: Environment Setup (Day 1)
1. Create S3 buckets for data storage
2. Run `setup-athena-workgroup.py` to create Athena workgroup with governance controls
3. Set up Redshift cluster and configure connectivity

### Phase 2: Data Foundation (Day 2)
1. Execute `athena-external-tables.sql` to create external tables
2. Verify data is accessible through Athena
3. Test partitioning strategy
4. Validate data quality metrics

### Phase 3: Dimensional Model (Day 3)
1. Execute `redshift-scd2-setup.sql` to enhance existing Day 16 dimensional model
2. The script will add date dimension and enhance existing tables for SCD Type 2
3. Execute date dimension population procedure
4. Update fact table with date foreign keys
5. Create analytics views with governance indicators

### Phase 4: Analytics Layer (Day 4)
1. Validate certified views in both Athena and Redshift
2. Test governance guardrails
3. Verify data quality metrics
4. Check ownership metadata

### Phase 5: Visualization (Day 5)
1. Set up QuickSight data sources (Athena and Redshift)
2. Create datasets from certified views
3. Build dashboard with governance indicators
4. Configure sharing and access controls

## Key Concepts Covered

### Serverless Analytics with Athena
- External tables and partitioning strategies
- Query optimization and cost management
- Integration with Glue Data Catalog
- Performance tuning techniques

### Dimensional Modeling with SCD Type 2
- Slowly Changing Dimension Type 2 implementation
- Fact table loading with referential integrity
- Star schema design principles
- Data quality validation

### Governance and Self-Service
- Certified datasets with steward approval
- Pre-built queries/views to prevent PII exposure
- Query cost limits to prevent runaway queries
- Quality score indicators and ownership labels

### QuickSight Dashboard Design
- Dashboard design best practices
- Governance indicators visualization
- Data quality score display
- Ownership labels and certification status

## Success Criteria

### Technical Success
- [ ] Athena queries complete within expected timeframes
- [ ] Redshift dimensional model properly implements SCD Type 2
- [ ] QuickSight dashboard loads in under 10 seconds
- [ ] Data quality score is 95% or higher
- [ ] All queries stay within cost limits

### Business Success
- [ ] Users can self-serve analytics without IT intervention
- [ ] Governance guardrails prevent unauthorized data access
- [ ] Dashboard provides actionable insights
- [ ] Data ownership is clearly documented
- [ ] Solution scales to handle increased data volumes

## Troubleshooting

### Common Issues

1. **Athena Query Performance**
   - Check partitioning strategy
   - Verify data is in columnar format (Parquet/ORC)
   - Reduce amount of data scanned with proper filters

2. **SCD Type 2 Implementation**
   - Verify effective/expiration date logic
   - Check for missing updates in dimension tables
   - Ensure proper joins when loading fact tables

3. **QuickSight Connectivity**
   - Verify IAM roles have proper permissions
   - Check VPC configuration if using private Redshift
   - Ensure data sources are properly configured

4. **Cost Management**
   - Monitor CloudWatch metrics for query usage
   - Set up alarms for high-cost queries
   - Review workgroup configuration settings

## Next Steps

After completing these hands-on exercises:

1. **Advanced Analytics**: Implement ML models for trip prediction
2. **Real-time Analytics**: Add streaming data capabilities
3. **Advanced Governance**: Implement data lineage and impact analysis
4. **Performance Optimization**: Fine-tune query performance and cost
5. **User Training**: Train business users on self-service analytics

## Resources

- [Amazon Athena Documentation](https://docs.aws.amazon.com/athena/)
- [Amazon Redshift Documentation](https://docs.aws.amazon.com/redshift/)
- [Amazon QuickSight Documentation](https://docs.aws.amazon.com/quicksight/)
- [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)
- [Athena Best Practices](https://docs.aws.amazon.com/athena/latest/ug/best-practices.html)
- [QuickSight Dashboard Design](https://docs.aws.amazon.com/quicksight/latest/user/designing-dashboards.html)