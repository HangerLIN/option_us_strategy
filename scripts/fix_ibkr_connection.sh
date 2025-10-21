#!/usr/bin/env bash
# IBKR 连接问题修复脚本

set -eo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

# 颜色定义
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}╔══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║          IBKR 连接问题修复工具                               ║${NC}"
echo -e "${BLUE}╚══════════════════════════════════════════════════════════════╝${NC}"
echo ""

# 检查当前状态
echo -e "${YELLOW}📊 检查当前状态...${NC}"
echo ""

# 检查 IBKR 端口
IBKR_PORT=$(grep "^IB_PORT=" .env 2>/dev/null | cut -d'=' -f2 || echo "7497")
echo -e "  当前配置端口: ${IBKR_PORT}"

# 检查端口连通性
if nc -zv localhost 4002 2>&1 | grep -q succeeded; then
    echo -e "  ${GREEN}✅${NC} IB Gateway (4002) 可达"
    GATEWAY_AVAILABLE=true
elif nc -zv localhost 7497 2>&1 | grep -q succeeded; then
    echo -e "  ${GREEN}✅${NC} IBKR TWS (7497) 可达"
    GATEWAY_AVAILABLE=false
else
    echo -e "  ${RED}❌${NC} 未检测到 IBKR 服务"
    GATEWAY_AVAILABLE=false
fi

echo ""
echo -e "${BLUE}可用的解决方案:${NC}"
echo ""
echo -e "${GREEN}1.${NC} 快速修复 - 重启服务（TWS 需已重启）"
echo -e "${GREEN}2.${NC} 切换到 IB Gateway 端口 4002（推荐）"
echo -e "${GREEN}3.${NC} 查看详细诊断信息"
echo -e "${GREEN}4.${NC} 临时解决 - 不连接 IBKR 运行 exec_svc"
echo -e "${GREEN}5.${NC} 退出"
echo ""

read -p "请选择 (1-5): " choice

case $choice in
    1)
        echo ""
        echo -e "${YELLOW}🔄 重启所有服务...${NC}"
        ./scripts/manage_services.sh stop
        sleep 2
        ./scripts/manage_services.sh start
        ;;
    
    2)
        echo ""
        if [ "$GATEWAY_AVAILABLE" = true ]; then
            echo -e "${GREEN}✅ 检测到 IB Gateway 正在运行${NC}"
            echo -e "${YELLOW}📝 更新配置文件...${NC}"
            
            # 备份配置
            cp .env .env.backup.$(date +%Y%m%d_%H%M%S)
            
            # 更新端口
            sed -i.tmp 's/^IB_PORT=.*/IB_PORT=4002/' .env && rm -f .env.tmp
            
            echo -e "${GREEN}✅ 配置已更新: IB_PORT=4002${NC}"
            echo ""
            echo -e "${YELLOW}🔄 重启服务...${NC}"
            
            ./scripts/manage_services.sh stop
            sleep 2
            ./scripts/manage_services.sh start
            
            echo ""
            echo -e "${GREEN}✅ 完成！服务已切换到 IB Gateway${NC}"
        else
            echo -e "${YELLOW}⚠️  未检测到 IB Gateway (端口 4002)${NC}"
            echo ""
            echo "请先启动 IB Gateway Paper Trading，然后重新运行此脚本"
            echo ""
            echo "下载地址: https://www.interactivebrokers.com/en/trading/ibgateway-stable.php"
        fi
        ;;
    
    3)
        echo ""
        echo -e "${BLUE}═══ 诊断信息 ═══${NC}"
        echo ""
        echo "IBKR 连接日志:"
        echo "─────────────────────────────────────────────"
        grep -h "IBKR\|clientId" logs/manual/*.log 2>/dev/null | tail -20 || echo "无日志"
        echo ""
        echo "当前运行的服务:"
        echo "─────────────────────────────────────────────"
        ./scripts/manage_services.sh status
        echo ""
        echo "端口占用情况:"
        echo "─────────────────────────────────────────────"
        for port in 7497 7496 4002 4001; do
            if nc -zv localhost $port 2>&1 | grep -q succeeded; then
                echo "  ✅ 端口 $port 已被占用"
            else
                echo "  ❌ 端口 $port 未被占用"
            fi
        done
        ;;
    
    4)
        echo ""
        echo -e "${YELLOW}⚙️  配置 exec_svc 在测试模式运行（不连接 IBKR）${NC}"
        echo ""
        echo "修改 .env 添加测试模式标志..."
        
        # 检查是否已有 APP_ENV
        if grep -q "^APP_ENV=" .env; then
            echo -e "${YELLOW}已存在 APP_ENV 配置${NC}"
        else
            echo "" >> .env
            echo "# 临时测试模式（exec_svc 不连接 IBKR）" >> .env
            echo "# APP_ENV=test" >> .env
            echo -e "${GREEN}✅ 已添加配置注释${NC}"
        fi
        
        echo ""
        echo "📝 要启用测试模式，请手动编辑 .env:"
        echo "   APP_ENV=test"
        echo ""
        echo "⚠️  注意: 测试模式下 exec_svc 不会连接 IBKR，无法执行真实订单"
        ;;
    
    5)
        echo ""
        echo "退出"
        exit 0
        ;;
    
    *)
        echo ""
        echo -e "${RED}无效选择${NC}"
        exit 1
        ;;
esac

echo ""
echo -e "${GREEN}════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}✨ 操作完成${NC}"
echo ""
echo "查看服务状态: ./scripts/manage_services.sh status"
echo "查看日志: ./scripts/manage_services.sh logs exec_svc"
echo ""





