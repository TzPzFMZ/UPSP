# Scripts

当前版本（自动版 v1.6）的脚本文件存放在 `examples/FMA/` 目录下，进入该目录即可直接运行：

```bash
cd examples/FMA
python UPSP_agent.py --root . --input "你好"
python UPSP.py --root .
```

**为什么脚本不在这里？**

自动版脚本依赖与位格文件同目录运行，放在示例目录里是最直接的用法。`scripts/` 目录当前作为占位保留。

官方版发布后，脚本将迁移至此，支持标准路径调用与多实例管理。