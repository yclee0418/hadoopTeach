#!/bin/bash
#下面设置输入文件，把用户执行pre_deal.sh命令时提供的第一个参数作为输入文件名称
infile=$1
fetchSize=$3
fetchNum=$4
#keep header into file
head -1 $infile > head.txt
#下面删除檔案中的第1行並輸出至暫存檔
cat $infile | sed '1d' $infile > tmp.txt
#下面设置输出文件，把用户执行pre_deal.sh命令时提供的第二个参数作为输出文件名称
outfile=$2
#注意！！最后的$infile > $outfile必须跟在}’这两个字符的后面
awk -v fetchSize=$fetchSize -v fetchNum=$fetchNum -F "," 'BEGIN{
        srand();
        id=0;
	id1=0;
    }
    {
	id=id+1;
        value=int(rand()*int(fetchSize)); 
	if ( value == int(fetchNum) ) {
		id1=id1+1;       
		print id1","$0;
	}
	
    }' tmp.txt > $outfile
#刪除暫存檔
rm -f tmp.txt
