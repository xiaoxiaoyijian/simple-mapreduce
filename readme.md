Simple Map/Reduce Framework written by GO programming language

参考：
MapReduce中的Map和Reduce操作的抽象描述

MapReduce借鉴了函数式程序设计语言Lisp中的思想，定义了如下的Map和Reduce两个抽象的编程接口，由用户去编程实现:

map: (k1; v1) → [(k2; v2)]
输入：键值对(k1; v1)表示的数据

处理：文档数据记录(如文本文件中的行，或数据表格中的行)将以“键值对”形式传入map函数；map函数将处理这些键值对，并以另一种键值对形式输出处理的一组键值对中间结果　　　[(k2; v2)]

输出：键值对[(k2; v2)]表示的一组中间数据

reduce: (k2; [v2]) → [(k3; v3)]
输入： 由map输出的一组键值对[(k2; v2)] 将被进行合并处理将同样主键下的不同数值合并到一个列表[v2]中，故reduce的输入为(k2; [v2])

处理：对传入的中间结果列表数据进行某种整理或进一步的处理,并产生最终的某种形式的结果输出[(k3; v3)] 。

输出：最终输出结果[(k3; v3)]
