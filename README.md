1. 使用方法
gcc -o main -pthread main.c -std=c99 

./main 

2. 整体思路
 先文件全部读入内存，多线程处理数据。
 
 每个线程一个hashmap来处理数据，总共num_thread个哈希表，主要是求和和计数，然后主线程来merge。
 
 
3. 优化思路：
  1）两级哈希表.一级哈希表的num_buckets=256，数据经过第一次哈希计算放入到不同的哈希表，总共num_threads*num_buckets个哈希表，最后进行合并。
  
