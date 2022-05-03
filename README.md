# Financial Research

![tensorflow](https://img.shields.io/badge/TensorFlow-FF6F00?style=for-the-badge&logo=tensorflow&logoColor=white) ![python](https://img.shields.io/badge/Python-FFD43B?style=for-the-badge&logo=python&logoColor=blue)

![overview](https://activity-graph.herokuapp.com/graph?username=ppoak&theme=minimal)

## 简介

这是与量化研究相关的项目仓库，其中每个量化、金融工程相关的研究都在项目中用文件夹单独列出，README文件中包含了相关项目的介绍、文献（文献链接一般都可获取），并对这些研报做复现、提升。

other文件夹中是一些杂乱的想法，还没有能够完全实现或是暂时没有好办法对效果进行提升的指标，这些指标优于散乱或是正处于开发阶段暂时没有使用独立的项目文件夹存放。

pandasquant是量化分析的综合库，包含了多种pandas数据分析组件，包括数据获取、数据预处理、回归分析、描述性统计分析、持仓回测等模块，具体使用方法详见[项目主页](https://github.com/ppoak/pandasquant/)

## [吸收比例](./absorb_ratio/README.md)

吸收比例是计算行业交易拥挤度的一种方法，使用了PCA的主成分分析方法，为行业交易拥挤度提供了可靠度量，加之以相对价值指标能够获得不错的回测结果。

## [财务风险比例](./financial_risk_ratio/README.md)

*目前本项目正在探索与持续更新中*，通过在不同时期的一系列的具有相对勾稽关系的财务报表科目之间的比值，由于同一行业中不同公司在这些具有勾稽关系的科目之间的比值应近似的服从一个正态分布的数学原理，筛选出行业中的指标比值离群点，并由此估算出个股、行业以及分板块的风险比例，以此为上市公司财务状态提供一些预警参考。

## [行业动量与反转效应](./industrial_momentum_and_reverse/README.md)

传统动量只考虑了收盘价到收盘价（close-close）的计算方式，而本项目将动量进一步的拆解和细分，将传统动量的拆分为隔夜收益（close-open）与日内收益（open-close）；同时，日内收益可以进一步划分，按照极端值的划分标准，如MAD去极值方法，将极端值与非极端值划分为极端收益（extreme return）与温和收益（gentle return）。从而进一步的对分解后的收益进行动量效应和反转效应的分析。

## [申万四象限模型](./sw_quadrant/README.md)

申万行业四象限利用行业间相对强弱的变化，近似的计算出变化率（一阶导）与变化率的变化率（二阶导），假设行业相对强弱是呈正弦曲线变化的，以此对行业的强弱变化进行进一步的预测和分析。

## 项目维护

目前项目正在不断添加新的想法和可以参考的研报内容，如果有不同的想法或是希望获取项目库中相关数据的可以联系邮箱`oakery@qq.com`，也欢迎发起PullRequest，任何问题可以Issue中提出。
