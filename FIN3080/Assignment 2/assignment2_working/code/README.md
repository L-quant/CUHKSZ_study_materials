# Python 代码目录

这个目录用于存放作业复现代码。

## 结构说明

- `run_all.py`
  - 总入口，后续用来一键执行全部流程
- `src/`
  - 主要 Python 模块
- `config/`
  - 数据路径和运行参数模板
- `outputs/tables/`
  - 输出的结果表
- `outputs/figures/`
  - 输出的图
- `logs/`
  - 运行日志

## 运行方式

后续完成代码后，预计在该目录下运行：

```bash
python run_all.py --config config/data_paths.example.json
```
