goods_table=$(echo $2 | cut -d"." -f 2)
bq query  --replace --use_legacy_sql=False --format=csv  "SELECT string_agg(lower(column_name)) columns  FROM  $1_unima.INFORMATION_SCHEMA.COLUMNS where table_name='$goods_table'" | awk 'NR>2' >a.txt
column_names=$(sed -i 's/"//g' a.txt | cat a.txt)
sql="SELECT '$1' as code_name, 'goods' as table_name,$column_names from $2 where availability='1'  "

echo $sql
bq query -n 0 --replace --use_legacy_sql=False --destination_table="$3" $sql

