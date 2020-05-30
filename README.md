#大数据词频统计实验


###详情见Experience report.pdf文件


###'CSDN'地址：https://blog.csdn.net/weixin_43850920/article/details/106449294

###'github'地址：https://github.com/dawudmq/big_data


##一、目标


###1.hadoop+spark完成词频统计

###2.对比HDFS与虚拟机内部存储系统处理速度

###3.spark本地模式下CPU配置个数对运算的影响


##二、数据说明


###1.input:Web of knowledge导出的txt文献信息2000篇

###2.output:Spark RDD文件，但实际上应该含有_SUSSESS文件,由于是0kb无法上传

###3.code:包含wordcount.scala用于批量处理数据，而wordcount_local.scala与wordcount_HDFS.scala分别用于本地文件系统和HDFS中单个文件的测试。

###4.Experience report.pdf:实验细节报告


