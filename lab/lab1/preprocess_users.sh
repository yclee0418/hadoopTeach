#!/bin/bash
#下面设置输入文件，把用户执行pre_deal.sh命令时提供的第一个参数作为输入文件名称
infile=$1
#下面删除檔案中的第1行並輸出至暫存檔
cat $infile | sed '1d' $infile > tmp.txt
#下面设置输出文件，把用户执行pre_deal.sh命令时提供的第二个参数作为输出文件名称
outfile=$2
#注意！！最后的$infile > $outfile必须跟在}’这两个字符的后面
awk -F "," 'BEGIN{
        srand();
        id=0;
        Province[0]="臺北市";Province[1]="新北市";Province[2]="桃園市";Province[3]="臺中市";Province[4]="臺南市";Province[5]="高雄市";Province[6]="基隆市";
        Province[7]="新竹市";Province[8]="嘉義市";Province[9]="新竹縣";Province[10]="苗栗縣";Province[11]="彰化縣";Province[12]="南投縣";Province[13]="雲林縣";
        Province[14]="嘉義縣";Province[15]="屏東縣";Province[16]="宜蘭縣";Province[17]="花蓮縣";Province[18]="臺東縣";Province[19]="澎湖縣";
    }
    {
        id=id+1;
        value=int(rand()*20);       
        print id"\t"$1"\t"$2"\t"$3"\t"$5"\t"substr($6,1,10)"\t"Province[value]
    }' tmp.txt > $outfile
#刪除暫存檔
rm -f tmp.txt
