pipeline {
    agent any
    stages { 
        stage('Checkout Deployment') {
            steps {
                dir("$WORKSPACE/satyrn-deployment") {
                    git(
                       branch: 'main',
                        credentialsId: 'dockerised',
                        url: 'git@github.com:nu-c3lab/satyrn-deployment.git'
                    )
                }
            }
        }

        stage('Checkout Templates') {
            steps {
                dir("$WORKSPACE/satyrn-templates") {
                    git(
                       branch: 'main',
                        credentialsId: 'git-cred-c3',
                        url: 'git@github.com:nu-c3lab/c3-satyrn-templates.git'
                    )
                }
            }
        }

        stage('Build') {
            steps {
                echo 'Starting docker build!'
                sh 'ls'
                sh 'docker build -t satyrn-api . --network=host'
            }
        }

        stage('Push') {
            steps{
            echo  'Logging in!'
                sh 'aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 304793330600.dkr.ecr.us-east-1.amazonaws.com'
                sh 'docker tag satyrn-api:latest 304793330600.dkr.ecr.us-east-1.amazonaws.com/satyrn-api:latest'
                sh 'docker push 304793330600.dkr.ecr.us-east-1.amazonaws.com/satyrn-api:latest'
                sh 'docker tag satyrn-api:latest 304793330600.dkr.ecr.us-east-1.amazonaws.com/satyrn-api:$BUILD_NUMBER'
                sh 'docker push 304793330600.dkr.ecr.us-east-1.amazonaws.com/satyrn-api:$BUILD_NUMBER'
            }
        }

        stage('Deploy') {
          dir("$WORKSPACE/satyrn-deployment")
            steps {
                sh 'helm upgrade --install satyrn-api charts/generic --values charts/satyrn-api/values-override.yaml --create-namespace --namespace dev-satyrn-api --set image.tag=$BUILD_NUMBER'
            }
          }
        }
    }
}

