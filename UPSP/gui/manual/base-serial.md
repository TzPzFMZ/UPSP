---
id: base-serial
title: Base 串行模型路由
page: settings
summary: Seed 保持逐帧串行，但不同阶段可以使用不同模型和独立降级链。
sourceRefs: LocalAppData/UPSP/config/models.json; Documents/UPSP/personas/<PID>/OS/config/model_routing.json; AGENTS.md
---

# Base 串行模型路由

Seed 仍按 `起手 → 反应 → 善后` 串行运行；一个 Frame 只调用有效模型链中的一个模型。这和“整个 UPSP 只能配置一个模型”不是一回事。

## 两层设置

- 右下角“全局设置／模型服务”管理服务连接、共享密钥和可复用模型配置。同一连接与密钥可以供多个模型配置使用。
- 左栏“位格设置／模型路由”决定当前位格的起手、反应和善后分别使用什么模型。
- 全局模型真源位于 Windows `LocalAppData\UPSP\config\models.json`，当前位格路由位于“文档”已知文件夹下的 `UPSP\personas\<PID>\OS\config\model_routing.json`；Runtime、CLI 与 GUI 共用现有 `ConfigStore`，没有 persona 局部覆盖层或第二套密钥库。

## 三乘三路由

每个阶段都有主模型、备用一、备用二。反应主模型留空时继承起手，善后主模型留空时继承反应的有效主模型；备用格不继承。界面会同时显示显式选择、继承来源和最终降级顺序。

开启“允许跨阶段模型容灾”后，当前阶段显式备用仍优先，只用其他阶段的有效主模型补齐空槽；同一模型配置或相同 URL/model/key 指纹不会被重复计算。每个模型最多请求三次，每阶段最多三个不同模型，因此单阶段硬上限为九次。

密钥环境变量优先于本机文件值。设置读取、页面、日志和证据只显示密钥是否存在，永不回显正文。没有密钥或模型服务不可用时，全局设置仍可打开和编辑。

本地桌面入口复用 `python tools/serve_seed_gui.py --open`。它只是 stdlib localhost 宿主和浏览器入口，不是后台常驻安装器；关闭终端即可停止服务。

右侧只展示真实 Round 与 Frames 投影；模型目录和位格路由都从设置入口管理，不在运行概览中制造额外工作单元。
