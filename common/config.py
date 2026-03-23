#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
配置管理公共模块

统一的配置文件加载、环境变量支持和参数解析
"""

import argparse
import json
import os
from typing import Dict, Any, Optional
from pathlib import Path


def load_config_file(config_path: str) -> Dict[str, Any]:
    """加载 JSON 配置文件"""
    if not config_path or not Path(config_path).exists():
        return {}
    
    with open(config_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    return data if isinstance(data, dict) else {}


def get_config_value(
    cli_value: Optional[str],
    config_value: Optional[str],
    env_var: str,
    default: Optional[str] = None
) -> Optional[str]:
    """获取配置值，优先级：CLI > 配置文件 > 环境变量 > 默认值"""
    if cli_value is not None and cli_value != '':
        return cli_value
    if config_value is not None and config_value != '':
        return config_value
    return os.getenv(env_var, default)


class BaseConfig:
    """基础配置类"""
    
    def __init__(
        self,
        console_url: str = "http://localhost:9000",
        username: str = "",
        password: str = "",
        timeout: int = 60,
        insecure: bool = False,
    ):
        self.console_url = console_url
        self.username = username
        self.password = password
        self.timeout = timeout
        self.insecure = insecure
    
    @classmethod
    def from_args_and_config(cls, args: argparse.Namespace, config: Dict[str, Any]):
        """从命令行参数和配置文件创建配置"""
        auth_config = config.get('auth', {}) if isinstance(config.get('auth'), dict) else {}
        
        return cls(
            console_url=get_config_value(
                getattr(args, 'console', None) or getattr(args, 'host', None),
                config.get('consoleUrl') or config.get('baseUrl'),
                'CONSOLE_URL',
                'http://localhost:9000'
            ),
            username=get_config_value(
                getattr(args, 'username', None),
                auth_config.get('username'),
                'CONSOLE_USERNAME',
                ''
            ),
            password=get_config_value(
                getattr(args, 'password', None),
                auth_config.get('password'),
                'CONSOLE_PASSWORD',
                ''
            ),
            timeout=int(get_config_value(
                str(getattr(args, 'timeout', 60)),
                str(config.get('timeout', 60)),
                'CONSOLE_TIMEOUT',
                '60'
            )),
            insecure=getattr(args, 'insecure', False) or config.get('insecure', False),
        )


def add_common_args(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    """添加通用参数到 ArgumentParser"""
    
    # Console 连接参数
    conn_group = parser.add_argument_group('Console 连接参数')
    conn_group.add_argument(
        '-c', '--console',
        default='http://localhost:9000',
        help='INFINI Console URL (默认: http://localhost:9000, 环境变量: CONSOLE_URL)'
    )
    conn_group.add_argument(
        '-u', '--username',
        default='',
        help='登录用户名 (环境变量: CONSOLE_USERNAME)'
    )
    conn_group.add_argument(
        '-p', '--password',
        default='',
        help='登录密码 (环境变量: CONSOLE_PASSWORD)'
    )
    conn_group.add_argument(
        '--timeout',
        type=int,
        default=60,
        help='请求超时时间(秒) (默认: 60, 环境变量: CONSOLE_TIMEOUT)'
    )
    conn_group.add_argument(
        '--insecure',
        action='store_true',
        help='忽略 SSL 证书验证'
    )
    
    # 配置文件参数
    config_group = parser.add_argument_group('配置参数')
    config_group.add_argument(
        '--config',
        default='',
        help='配置文件路径 (JSON 格式)'
    )
    
    # 输出参数
    output_group = parser.add_argument_group('输出参数')
    output_group.add_argument(
        '-o', '--output',
        default='',
        help='输出文件或目录路径'
    )
    
    return parser


def load_and_merge_config(args: argparse.Namespace) -> tuple:
    """加载配置文件并合并参数
    
    Returns:
        (merged_config_dict, effective_args)
    """
    config = {}
    if getattr(args, 'config', None) and Path(args.config).exists():
        config = load_config_file(args.config)
    
    return config, args
