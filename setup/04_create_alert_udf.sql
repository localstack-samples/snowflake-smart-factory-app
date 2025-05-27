-- Simple Alert System: One UDF that sends email report of all critical machines
USE DATABASE FACTORY_PIPELINE_DEMO;
USE SCHEMA PUBLIC;

-- Create alerts log table
CREATE TABLE IF NOT EXISTS ALERT_LOG (
    alert_id NUMBER AUTOINCREMENT,
    total_critical_machines NUMBER,
    email_sent BOOLEAN DEFAULT FALSE,
    message_id VARCHAR(100),
    alert_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Simple UDF to send email report of all critical machines
CREATE OR REPLACE FUNCTION send_critical_machines_report()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = 3.9
PACKAGES = ('boto3')
HANDLER = 'send_report'
AS $$
import boto3
import json
from datetime import datetime

def send_report():
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
        
        # Mock critical machines data (in real scenario, this would query the database)
        # For now, we'll simulate some critical machines
        critical_machines = [
            {"machine_id": "MACHINE_001", "risk_score": 95.5, "issue": "High temperature detected"},
            {"machine_id": "MACHINE_003", "risk_score": 87.2, "issue": "Excessive vibration"},
            {"machine_id": "MACHINE_007", "risk_score": 92.8, "issue": "Pressure anomaly"}
        ]
        
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