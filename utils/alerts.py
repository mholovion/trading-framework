import asyncio
import smtplib
import aiohttp
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, Any, List
import logging
from core.exceptions import ConfigurationError

class AlertManager:
    """Manages system alerting"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config['alerting']
        self.logger = logging.getLogger(__name__)
        
        if not self.config['enabled']:
            self.logger.info("Alerting disabled")
            return
        
        self.email_config = self.config['email']
        self.webhook_config = self.config['webhook']
    
    async def send_alert(self, severity: str, title: str, message: str, metadata: Dict[str, Any] = None):
        """Send alert through configured channels"""
        if not self.config['enabled']:
            return
        
        alert_data = {
            'severity': severity,
            'title': title,
            'message': message,
            'metadata': metadata or {},
            'timestamp': asyncio.get_event_loop().time()
        }
        
        tasks = []
        
        if self.email_config['enabled']:
            tasks.append(self._send_email_alert(alert_data))
        
        if self.webhook_config['enabled']:
            tasks.append(self._send_webhook_alert(alert_data))
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _send_email_alert(self, alert_data: Dict[str, Any]):
        """Send email alert"""
        try:
            # Create message
            msg = MIMEMultipart()
            msg['From'] = self.email_config['username']
            msg['To'] = ', '.join(self.email_config['recipients'])
            msg['Subject'] = f"[{alert_data['severity']}] {alert_data['title']}"
            
            # Create body
            body = f"""
Severity: {alert_data['severity']}
Title: {alert_data['title']}
Message: {alert_data['message']}
Timestamp: {alert_data['timestamp']}

Metadata:
{alert_data['metadata']}
            """
            
            msg.attach(MIMEText(body, 'plain'))
            
            # Send email
            server = smtplib.SMTP(self.email_config['smtp_host'], self.email_config['smtp_port'])
            server.starttls()
            server.login(self.email_config['username'], self.email_config['password'])
            server.send_message(msg)
            server.quit()
            
            self.logger.info(f"Email alert sent: {alert_data['title']}")
            
        except Exception as e:
            self.logger.error(f"Failed to send email alert: {e}")
    
    async def _send_webhook_alert(self, alert_data: Dict[str, Any]):
        """Send webhook alert"""
        try:
            timeout = aiohttp.ClientTimeout(total=self.webhook_config['timeout'])
            
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(self.webhook_config['url'], json=alert_data) as response:
                    if response.status == 200:
                        self.logger.info(f"Webhook alert sent: {alert_data['title']}")
                    else:
                        self.logger.error(f"Webhook alert failed with status {response.status}")
                        
        except Exception as e:
            self.logger.error(f"Failed to send webhook alert: {e}")