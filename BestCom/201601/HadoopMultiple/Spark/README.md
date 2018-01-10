Spark 2.2.1 多節點叢集設定
1. master － hadoop1
2. slaves - hadoop2, hadoop3
3. 目前的配置下(master 4g RAM, slaves 3g RAM)如要以yarn作為ResourceManager，需在yarn-site.xml中加入yarn.nodemanager.vmem-check-enabled等設定，不然會發生beyond virtual memory limits錯誤
