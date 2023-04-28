# airflow-JP
Este projeto consiste na realização de orquestrações de tarefas por meio do Airflow, para uma melhor otimização e fácil repetição em diversos outros sistemas, o Airflow foi rodado dentro de containers Docker.

![Projeto JP (1)](https://user-images.githubusercontent.com/101363298/233790800-43152c93-3147-4952-b918-a78537086d61.jpeg)

O projeto faz uso de pacotes disponibilizados pelo próprio Airflow, como S3ListOperator, S3DeleteObjectsOperator, S3KeySensor e S3Hook, que facilitam a integração com um ambiente dentro do AWS. Além disso, outras bibliotecas Python foram utilizadas para uma maior integração com o AWS, como Pandas, AWS Wrangler e boto3.

Dentro do Airflow, foi criada uma DAG para orquestrar a ingestão de arquivos .txt disponibilizados em um bucket S3, fazendo primeiro a verificação de existência ou não de um arquivo no diretório. Havendo um arquivo ou múltiplos, esses são listados pelo S3ListOperator e, posteriormente, compartilhados para a tarefa de processamento dos dados, por meio do XComs ("cross-communications") do Airflow.

![dag airflow](https://user-images.githubusercontent.com/101363298/233791583-4908fdf8-d8f4-4b37-8756-74ee4c0138b4.PNG)

Após a listagem de todos esses arquivos (s3 Keys), eles são processados conforme o seu layout e, por fim, armazenados em um novo S3 Bucket, porém como .parquet. Esse processamento é realizado por meio da utilização de Dynamic Task Mapping do Airflow, onde serão criadas tarefas para cada arquivo, garantindo assim o processamento de um ou múltiplos arquivos de forma automática, além de melhor visibilidade e garantia de que, caso haja erro com um arquivo, o restante poderá ser processado sem problemas.

Para garantir que não haja leitura e/ou processamento de arquivos já processados, os arquivos dentro do primeiro S3 serão deletados, também utilizando Dynamic Task Mapping do Airflow. Porém, apenas após os todos arquivos serem processados com sucesso.

Por fim, para visualização dos arquivos recém processados em um novo S3 Bucket será necessário a criação de uma tabela no AWS Athena.

![athena](https://user-images.githubusercontent.com/101363298/233791618-14119e27-5c0e-41c5-bb4f-64db42af54ed.jpg)

Esse projeto tem como objetivo proporcionar uma solução de orquestração de tarefas automatizada, permitindo uma maior visibilidade, garantia e facilidade de integração com o ambiente AWS.
