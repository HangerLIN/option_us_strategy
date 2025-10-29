#!/usr/bin/env python3
"""
简单的告警Webhook接收服务示例

这是一个基本的Flask应用，用于接收Alertmanager的告警通知。
你可以在此基础上扩展，实现发送到钉钉、企业微信、Slack等。

使用方法:
    python alert_webhook_example.py

然后在alertmanager.yml中配置:
    webhook_configs:
      - url: 'http://host.docker.internal:5000/alerts/webhook'
"""

from datetime import datetime
from flask import Flask, request, jsonify
import json
import logging

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def format_alert_message(alert: dict) -> str:
    """格式化告警消息"""
    severity_emoji = {
        "critical": "🚨",
        "high": "⚠️",
        "medium": "ℹ️",
        "low": "💡"
    }
    
    labels = alert.get("labels", {})
    annotations = alert.get("annotations", {})
    status = alert.get("status", "unknown")
    
    severity = labels.get("severity", "unknown")
    emoji = severity_emoji.get(severity, "📢")
    
    message = f"""
{emoji} 告警通知
━━━━━━━━━━━━━━━━━━━━━━━━━━
告警名称: {labels.get('alertname', 'Unknown')}
告警级别: {severity.upper()}
服务: {labels.get('service', 'Unknown')}
类别: {labels.get('category', 'Unknown')}
状态: {status}

摘要: {annotations.get('summary', 'No summary')}
描述: {annotations.get('description', 'No description')}

故障排查: {annotations.get('runbook', 'No runbook')}
━━━━━━━━━━━━━━━━━━━━━━━━━━
触发时间: {alert.get('startsAt', 'Unknown')}
"""
    if status == "resolved":
        message += f"恢复时间: {alert.get('endsAt', 'Unknown')}\n"
    
    return message.strip()


def send_to_dingtalk(webhook_url: str, message: str):
    """发送到钉钉群机器人"""
    import requests
    
    payload = {
        "msgtype": "markdown",
        "markdown": {
            "title": "系统告警",
            "text": message
        }
    }
    
    try:
        response = requests.post(webhook_url, json=payload, timeout=5)
        response.raise_for_status()
        logger.info("钉钉通知发送成功")
    except Exception as e:
        logger.error(f"钉钉通知发送失败: {e}")


def send_to_wechat(corp_id: str, agent_id: str, secret: str, message: str):
    """发送到企业微信"""
    # 这里是示例代码，需要根据企业微信API实现
    logger.info("企业微信通知功能待实现")


@app.route('/alerts/webhook', methods=['POST'])
def handle_alert():
    """处理Alertmanager webhook"""
    try:
        data = request.get_json()
        
        logger.info(f"收到告警通知，共 {len(data.get('alerts', []))} 条")
        logger.debug(f"原始数据: {json.dumps(data, indent=2)}")
        
        # 处理每个告警
        for alert in data.get('alerts', []):
            message = format_alert_message(alert)
            logger.info(f"\n{message}\n")
            
            # 这里可以添加发送到各种通知渠道的逻辑
            # send_to_dingtalk("YOUR_DINGTALK_WEBHOOK", message)
            # send_to_wechat("CORP_ID", "AGENT_ID", "SECRET", message)
        
        return jsonify({"status": "success"}), 200
    
    except Exception as e:
        logger.error(f"处理告警失败: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/alerts/critical', methods=['POST'])
def handle_critical_alert():
    """处理Critical级别告警"""
    try:
        data = request.get_json()
        logger.critical(f"🚨 Critical告警: {len(data.get('alerts', []))} 条")
        
        for alert in data.get('alerts', []):
            message = format_alert_message(alert)
            logger.critical(f"\n{message}\n")
            
            # Critical级别应该发送到所有通知渠道
            # send_to_dingtalk("CRITICAL_WEBHOOK", message)
            # send_sms("ONCALL_PHONE", message)
            # send_phone_call("ONCALL_PHONE")
        
        return jsonify({"status": "success"}), 200
    
    except Exception as e:
        logger.error(f"处理Critical告警失败: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/alerts/high', methods=['POST'])
def handle_high_alert():
    """处理High级别告警"""
    try:
        data = request.get_json()
        logger.warning(f"⚠️  High告警: {len(data.get('alerts', []))} 条")
        
        for alert in data.get('alerts', []):
            message = format_alert_message(alert)
            logger.warning(f"\n{message}\n")
        
        return jsonify({"status": "success"}), 200
    
    except Exception as e:
        logger.error(f"处理High告警失败: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/alerts/medium', methods=['POST'])
def handle_medium_alert():
    """处理Medium级别告警"""
    try:
        data = request.get_json()
        logger.info(f"ℹ️  Medium告警: {len(data.get('alerts', []))} 条")
        
        for alert in data.get('alerts', []):
            message = format_alert_message(alert)
            logger.info(f"\n{message}\n")
        
        return jsonify({"status": "success"}), 200
    
    except Exception as e:
        logger.error(f"处理Medium告警失败: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/alerts/low', methods=['POST'])
def handle_low_alert():
    """处理Low级别告警"""
    try:
        data = request.get_json()
        logger.info(f"💡 Low告警: {len(data.get('alerts', []))} 条")
        
        for alert in data.get('alerts', []):
            message = format_alert_message(alert)
            logger.debug(f"\n{message}\n")
        
        return jsonify({"status": "success"}), 200
    
    except Exception as e:
        logger.error(f"处理Low告警失败: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/health', methods=['GET'])
def health_check():
    """健康检查端点"""
    return jsonify({
        "status": "healthy",
        "service": "alert-webhook",
        "timestamp": datetime.utcnow().isoformat()
    }), 200


if __name__ == '__main__':
    logger.info("启动告警Webhook服务...")
    logger.info("监听地址: http://0.0.0.0:5000")
    logger.info("Webhook端点:")
    logger.info("  - http://localhost:5000/alerts/webhook")
    logger.info("  - http://localhost:5000/alerts/critical")
    logger.info("  - http://localhost:5000/alerts/high")
    logger.info("  - http://localhost:5000/alerts/medium")
    logger.info("  - http://localhost:5000/alerts/low")
    
    app.run(host='0.0.0.0', port=5000, debug=True)












