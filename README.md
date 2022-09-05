# 下载spark安装包，解压
    wget https://dlcdn.apache.org/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz

# 加入以下包到jars目录
    wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.1/hadoop-common-3.3.1.jar
    wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.235/aws-java-sdk-bundle-1.12.235.jar
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-s3/1.12.235/aws-java-sdk-s3-1.12.235.jar
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.12.235/aws-java-sdk-1.12.235.jar
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-core/1.12.235/aws-java-sdk-core-1.12.235.jar


# 打镜像x86
    aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin xxxxxxx.dkr.ecr.us-east-1.amazonaws.com
    ./bin/docker-image-tool.sh -r xxxxxxx.dkr.ecr.us-east-1.amazonaws.com -t 3.2.1-x86-test-v0.1 build
    ./bin/docker-image-tool.sh -r xxxxxxx.dkr.ecr.us-east-1.amazonaws.com -t 3.2.1-x86-test-v0.1 push

> **jdk8镜像**  
./bin/docker-image-tool.sh -r xxxxxxx.dkr.ecr.us-east-1.amazonaws.com -t 3.2.1-x86-jdk8-test-v0.1 build
./bin/docker-image-tool.sh -r xxxxxxx.dkr.ecr.us-east-1.amazonaws.com -t 3.2.1-x86-jdk8-test-v0.1 push

# 打镜像arm
    aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin xxxxxxx.dkr.ecr.us-east-1.amazonaws.com
    docker build -t xxxxxxx.dkr.ecr.us-east-1.amazonaws.com/spark:3.2.1-arm-test-v0.1 .
    docker push xxxxxxx.dkr.ecr.us-east-1.amazonaws.com/spark:3.2.1-arm-test-v0.1
> **jdk8镜像**
docker build -f Dockerfile_jdk8 -t xxxxxxx.dkr.ecr.us-east-1.amazonaws.com/spark:3.2.1-arm-jdk8-test-v0.1 .
docker push xxxxxxx.dkr.ecr.us-east-1.amazonaws.com/spark:3.2.1-arm-jdk8-test-v0.1

# 部署eks集群
    eksctl create cluster -f ./eks/eksctl.yaml
    eksctl utils associate-iam-oidc-provider --region=us-east-1 --cluster=spark-eks-test1 --approve
    aws eks --region us-east-1 update-kubeconfig --name spark-eks-test1

# 部署集群自动伸缩
    kubectl create -f ./eks/cluster_autoscaler.yml
    kubectl apply -f ./eks/cluster_autoscaler.yml

# 建账号策略
arn:aws:iam::xxxxxxx:policy/spark_eks_policy

    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "SourcePermissions",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:ListBucket"
                ],
                "Resource": [
                    "arn:aws:s3:::nyc-tlc/*",
                    "arn:aws:s3:::nyc-tlc"
                ]
            },
            {
                "Sid": "TargetPermissions",
                "Effect": "Allow",
                "Action": [
                    "s3:PutObject",
                    "s3:GetObject",
                    "s3:AbortMultipartUpload",
                    "s3:DeleteObject",
                    "s3:ListMultipartUploadParts",
                    "s3:listBucketMultipartUploads",
                    "s3:ListBucket"
                ],
                "Resource": [
                    "arn:aws:s3:::bucketnameeksout/*",
                    "arn:aws:s3:::bucketnameeksout"
                ]
            }
        ]
    }

# 建空间
    kubectl create namespace spark

# 建账号
    kubectl create serviceaccount spark -n spark or
    eksctl create iamserviceaccount \
    --name spark \
    --namespace spark \
    --cluster spark-eks-test1 \
    --attach-policy-arn arn:aws:iam::xxxxxxx:policy/spark_eks_policy \
    --approve --override-existing-serviceaccounts

# 绑定账号
    kubectl create clusterrolebinding spark-role \
    --clusterrole=edit \
    --serviceaccount=spark:spark

# 任务提交
/home/ec2-user/spark/spark-3.2.1-bin-hadoop3.2/bin/spark-submit \
--master k8s://https://xxxxxxx.gr7.us-east-1.eks.amazonaws.com  \
--deploy-mode cluster \
--name java_demo \
--class mydemo.spark.JavaSQLDataSourceExample \
--conf spark.executor.instances=3 \
--conf spark.hadoop.fs.s3a.downgrade.syncable.exceptions=false \
--conf spark.executor.memoryOverhead=4g \
--conf spark.executor.memory=4G \
--conf spark.kubernetes.executor.node.selector.node-lifecycle=spot \
#--conf spark.shuffle.service.enabled=true \
#--conf spark.dynamicAllocation.enabled=true \
#--conf spark.dynamicAllocation.shuffleTracking.enabled=true \
#--conf spark.dynamicAllocation.initialExecutors=10 \
#--conf spark.dynamicAllocation.maxExecutors=100 \
#--conf spark.dynamicAllocation.minExecutors=10 \
--conf spark.executor.memoryOverhead=1g \
--conf spark.executor.memory=1G \
--conf spark.kubernetes.node.selector.arch=$ARCH \
--conf spark.kubernetes.container.image=xxxxxxx.dkr.ecr.us-east-1.amazonaws.com/spark:3.2.1-x86-test-v0.1 \
--conf spark.kubernetes.file.upload.path=s3a://bucketname/test-spark-esk-01 \
--conf spark.hadoop.fs.s3a.access.key=xxxxxxxxxxxx \
--conf spark.hadoop.fs.s3a.secret.key=xxxxxxxxxxxx \
--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
--conf spark.hadoop.fs.s3a.fast.upload=true \
--conf spark.hadoop.fs.s3a.downgrade.syncable.exceptions=false \
--conf spark.hadoop.fs.s3a.block.size=268435456 \
--conf spark.history.provider=org.apache.hadoop.fs.s3a.S3AFileSystem \
--conf spark.eventLog.enabled=true \
--conf spark.driver.extraClassPath=/opt/spark/jars/ \
--conf spark.executor.extraClassPath=/opt/spark/jars/ \
--conf spark.eventLog.dir=s3a://bucketname/sparkhistory \
--conf spark.history.fs.logDirectory=s3a://bucketname/sparkhistory \
--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
--conf spark.kubernetes.namespace=spark \
-–conf spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps”
./java_demo-0.1.jar \
task1
