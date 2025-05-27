# 🚨 Python UDF + SES Alerting Demo Guide

## Overview
This demo showcases a **simple and reliable alerting system** using:
- **Python UDFs** with Boto3 for SES integration
- **LocalStack SES** for email notifications
- **Direct SQL queries** on machine health data
- **Streamlit UI** for monitoring and control

## 🎯 Demo Flow (60 seconds)

### 1. Setup & Initial State (10s)
```bash
# Start LocalStack and setup base system
make start
make setup-alerts
```

**Show:** Healthy dashboard with all machines in good condition

### 2. Generate Critical Data (15s)
```bash
# Generate critical sensor data and setup SES
make demo-critical
```

**Show:** 
- Critical sensor data uploaded to S3
- SES email identities verified
- 3 machines transitioning to CRITICAL status

### 3. Process Data Pipeline (15s)
```bash
# Run dbt to process new data
make dbt
```

**Show:**
- dbt transformations processing critical data
- Machine health metrics updated with CRITICAL status

### 4. Send Alert Emails (15s)
```bash
# Process critical alerts and send emails
make process-alerts
```

**Show:**
- Python UDF processes critical machines
- SES emails sent for each critical machine
- Alert log updated with results

### 5. Email Verification (5s)
```bash
# Check sent emails via LocalStack SES API
make check-emails
```

**Show:**
- Email alerts sent to maintenance team
- Rich HTML formatting with machine details
- Message IDs and delivery status

## 🔧 Technical Architecture

### Direct UDF Processing
```sql
-- View processes all critical machines directly
CREATE VIEW critical_alerts_processor AS
SELECT 
    machine_id,
    failure_risk_score,
    health_status,
    maintenance_recommendation,
    send_critical_alert(...) as alert_result
FROM machine_health_metrics
WHERE health_status = 'CRITICAL';

-- Query view to process all alerts at once
SELECT * FROM critical_alerts_processor;
```

### Python UDF with SES
```python
# UDF sends real emails via LocalStack SES
def send_alert(machine_id, risk_score, health_status, maintenance_recommendation):
    ses_client = boto3.client(
        "ses",
        endpoint_url="http://localhost:4566",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1"
    )
    
    # Send formatted email alert
    response = ses_client.send_email(...)
    return {"email_sent": True, "message_id": response['MessageId']}
```

## 📧 Email Alert Features

### Rich HTML Formatting
- **Critical status indicators** with red styling
- **Machine details** in structured format
- **Risk scores** and maintenance recommendations
- **Timestamp** and alert metadata

### Multi-recipient Support
- **Primary:** maintenance-team@smartfactory.com
- **CC:** ops-team@smartfactory.com
- **From:** factory-alerts@smartfactory.com

### LocalStack Integration
- **Auto-verified** email identities
- **Real-time** email sending
- **API access** to sent messages
- **Message persistence** for testing

## 🎮 Interactive Demo Commands

### Quick Demo
```bash
# Complete end-to-end demo
make demo-alerts

# Check results
make check-emails
```

### Step-by-step Demo
```bash
# 1. Generate critical data
make demo-critical

# 2. Process with dbt
make dbt

# 3. Send alerts
make process-alerts

# 4. Test SES connection
make test-udf

# 5. Check emails
make check-emails
```

### Manual Testing
```sql
-- Check critical machines
SELECT * FROM FACTORY_PIPELINE_DEMO.PUBLIC_marts.machine_health_metrics 
WHERE health_status = 'CRITICAL';

-- Process alerts manually
SELECT * FROM critical_alerts_processor;

-- View alert log
SELECT * FROM ALERT_LOG ORDER BY alert_timestamp DESC;

-- Test UDF directly
SELECT send_critical_alert(
    'MACHINE_001', 
    95.5, 
    'CRITICAL', 
    'Immediate shutdown required'
);

-- Test SES connection
SELECT test_ses_connection();
```

## 🔍 Monitoring & Debugging

### Streamlit UI Tabs
1. **🚨 Critical Alerts** - Direct critical machine monitoring
2. **📧 Email Status** - SES email status and content
3. **📋 Alert Log** - Historical alert processing
4. **⚙️ Settings** - Configuration and testing

### LocalStack Logs
```bash
# Monitor SES activity
localstack logs | grep -i ses

# Check email sending
curl localhost:4566/_aws/ses | jq
```

### Database Monitoring
```sql
-- UDF status
SHOW FUNCTIONS LIKE '%alert%';

-- Alert processing stats
SELECT 
    COUNT(*) as total_alerts,
    SUM(CASE WHEN email_sent THEN 1 ELSE 0 END) as emails_sent,
    COUNT(DISTINCT machine_id) as affected_machines
FROM ALERT_LOG;

-- Critical machines count
SELECT COUNT(*) as critical_machines 
FROM FACTORY_PIPELINE_DEMO.PUBLIC_marts.machine_health_metrics 
WHERE health_status = 'CRITICAL';
```

## 🚀 Key Demo Points

### Simple & Reliable
- **No complex streams** - just direct SQL queries
- **Immediate processing** - call UDF when needed
- **Clear error handling** - easy to debug

### Email Integration
- **Actual email sending** via LocalStack SES
- **Rich formatting** with HTML and text versions
- **Error handling** and retry logic

### Production-ready Features
- **Alert logging** with full audit trail
- **Configurable thresholds** 
- **Multi-channel notifications**
- **Easy testing** with dedicated test functions

## 🎯 Success Metrics

### Demo Success Indicators
- ✅ UDF processes 3 critical machines
- ✅ 3 emails sent via SES
- ✅ Alert log shows successful processing
- ✅ UI displays real-time status
- ✅ No complex dependencies

### Performance Metrics
- **Processing time:** < 5 seconds
- **Email delivery:** < 2 seconds
- **UI responsiveness:** Immediate updates
- **Error rate:** 0% for demo data

## 🔧 Troubleshooting

### Common Issues
1. **UDF not found:** Run `make setup-alerts`
2. **SES errors:** Check LocalStack connectivity
3. **No emails:** Verify SES identities
4. **UI errors:** Check Snowflake connection

### Debug Commands
```bash
# Test LocalStack connectivity
curl localhost:4566/health

# Verify SES service
awslocal ses list-identities

# Check Snowflake connection
snow sql -q "SELECT CURRENT_VERSION()" -c localstack

# Test SES connection via UDF
snow sql -q "SELECT test_ses_connection()" -c localstack
```

## 🎉 Why This Approach Works

### Simplicity
- **No streams complexity** - just query and process
- **Direct control** - call alerts when you want
- **Easy debugging** - clear execution path

### Reliability
- **Fewer moving parts** - less to break
- **Immediate feedback** - see results instantly
- **Clear error messages** - easy troubleshooting

### Flexibility
- **On-demand processing** - run alerts anytime
- **Easy customization** - modify UDF as needed
- **Scalable** - process any number of machines

This demo showcases a **production-ready alerting system** that's simple, reliable, and easy to understand! 