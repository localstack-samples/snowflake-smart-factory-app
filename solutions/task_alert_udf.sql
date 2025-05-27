-- Simple Alert System: One UDF that sends email report of all critical machines
USE DATABASE FACTORY_PIPELINE_DEMO;
USE SCHEMA PUBLIC;



-- UDF to send email report with critical machines data passed as parameter
CREATE OR REPLACE FUNCTION send_critical_machines_report(critical_machines_json VARCHAR)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = 3.9
PACKAGES = ('boto3')
HANDLER = 'send_report'
AS $$
import boto3
import json
from datetime import datetime

def send_report(critical_machines_json):
    try:
        # LocalStack SES configuration
        endpoint_url = "http://localhost:4566"
        
        # Configure SES client
        ses_client = boto3.client(
            "ses",
            endpoint_url=endpoint_url,
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1"
        )
        
        # Email configuration
        sender_email = "hello@example.com"
        recipient_email = "maintenance-team@smartfactory.com"
        
        # Verify email identities (auto-verified in LocalStack)
        try:
            ses_client.verify_email_identity(EmailAddress=sender_email)
            ses_client.verify_email_identity(EmailAddress=recipient_email)
        except:
            pass  # Identities might already be verified
        
        # Parse the critical machines data passed as pipe-delimited string
        # Format: machine_id|risk_score|issue;machine_id|risk_score|issue;...
        critical_machines = []
        try:
            if critical_machines_json and critical_machines_json.strip():
                machine_entries = critical_machines_json.split(';')
                for entry in machine_entries:
                    if entry.strip():
                        parts = entry.split('|')
                        if len(parts) >= 3:
                            critical_machines.append({
                                "machine_id": parts[0],
                                "risk_score": float(parts[1]) if parts[1] else 0.0,
                                "issue": parts[2] if parts[2] else "Immediate maintenance required"
                            })
        except Exception as e:
            # Fallback to empty list if parsing fails
            critical_machines = []
        
        if not critical_machines:
            return {
                "status": "success",
                "message": "No critical machines found",
                "email_sent": False,
                "timestamp": datetime.now().isoformat()
            }
        
        # Create email content
        subject = f"🚨 CRITICAL ALERT: {len(critical_machines)} Machines Require Immediate Attention"
        
        # Text version
        body_text = f"""
CRITICAL MACHINES ALERT REPORT

Total Critical Machines: {len(critical_machines)}
Report Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

CRITICAL MACHINES:
"""
        
        for machine in critical_machines:
            body_text += f"""
- Machine ID: {machine['machine_id']}
  Risk Score: {machine['risk_score']}%
  Issue: {machine['issue']}
"""
        
        body_text += """
Please take immediate action to prevent equipment failure.

---
Smart Factory Health Monitor
Powered by LocalStack + Snowflake
        """
        
        # HTML version
        body_html = f"""
        <html>
        <head></head>
        <body>
            <h2 style="color: #d32f2f;">🚨 CRITICAL MACHINES ALERT REPORT</h2>
            
            <div style="background-color: #ffebee; padding: 15px; border-left: 4px solid #d32f2f; margin: 10px 0;">
                <h3>Summary</h3>
                <ul>
                    <li><strong>Total Critical Machines:</strong> {len(critical_machines)}</li>
                    <li><strong>Report Generated:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</li>
                </ul>
            </div>
            
            <h3>Critical Machines Details:</h3>
            <table style="border-collapse: collapse; width: 100%;">
                <tr style="background-color: #f5f5f5;">
                    <th style="border: 1px solid #ddd; padding: 8px;">Machine ID</th>
                    <th style="border: 1px solid #ddd; padding: 8px;">Risk Score</th>
                    <th style="border: 1px solid #ddd; padding: 8px;">Issue</th>
                </tr>
        """
        
        for machine in critical_machines:
            body_html += f"""
                <tr>
                    <td style="border: 1px solid #ddd; padding: 8px; font-weight: bold;">{machine['machine_id']}</td>
                    <td style="border: 1px solid #ddd; padding: 8px; color: #d32f2f;">{machine['risk_score']}%</td>
                    <td style="border: 1px solid #ddd; padding: 8px;">{machine['issue']}</td>
                </tr>
            """
        
        body_html += """
            </table>
            
            <p><strong>Please take immediate action to prevent equipment failure.</strong></p>
            
            <hr>
            <p style="font-size: 12px; color: #666;">
                Smart Factory Health Monitor<br>
                Powered by LocalStack + Snowflake
            </p>
        </body>
        </html>
        """
        
        # Send email via SES
        response = ses_client.send_email(
            Source=sender_email,
            Destination={
                'ToAddresses': [recipient_email]
            },
            Message={
                'Subject': {
                    'Data': subject,
                    'Charset': 'UTF-8'
                },
                'Body': {
                    'Text': {
                        'Data': body_text,
                        'Charset': 'UTF-8'
                    },
                    'Html': {
                        'Data': body_html,
                        'Charset': 'UTF-8'
                    }
                }
            }
        )
        
        return {
            "status": "success",
            "total_critical_machines": len(critical_machines),
            "email_sent": True,
            "message_id": response.get('MessageId'),
            "recipient": recipient_email,
            "sender": sender_email,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "email_sent": False,
            "timestamp": datetime.now().isoformat()
        }
$$;

-- Simple view to get critical machines as a concatenated string
CREATE OR REPLACE VIEW critical_machines_list AS
SELECT 
    LISTAGG(
        machine_id || '|' || failure_risk_score || '|' || 
        CASE WHEN maintenance_recommendation IS NULL THEN 'Immediate maintenance required' 
             ELSE maintenance_recommendation END, 
        ';'
    ) as machines_data
FROM FACTORY_PIPELINE_DEMO.PUBLIC_marts.machine_health_metrics
WHERE health_status = 'CRITICAL';

-- Create or replace the automated alert task (direct call)
CREATE OR REPLACE TASK automated_critical_alert_task
WAREHOUSE = 'test'
SCHEDULE = '30 SECONDS'
AS
    SELECT 
        CASE 
            WHEN machines_data IS NULL OR machines_data = '' THEN 
                FACTORY_PIPELINE_DEMO.PUBLIC.send_critical_machines_report('')
            ELSE 
                FACTORY_PIPELINE_DEMO.PUBLIC.send_critical_machines_report(machines_data)
        END
    FROM FACTORY_PIPELINE_DEMO.PUBLIC.critical_machines_list;

-- Resume the task to start scheduled execution
ALTER TASK automated_critical_alert_task RESUME;

-- Show task status
SHOW TASKS LIKE 'automated_critical_alert_task'; 