./install/hadoop/bin/hadoop \
jar \
target/Hadoop-0.0.1-SNAPSHOT.jar \
br.edu.ifba.gsort.hadoop.stackoverflow.MapReduce.Main \
output/stackoverflow \
input/stackoverflow/stackoverflow.com-Users-reduced \
input/stackoverflow/stackoverflow.com-Badges-reduced-joined \
input/stackoverflow/stackoverflow.com-Posts-reduced-joined \
input/stackoverflow/stackoverflow.com-Comments-reduced-joined
