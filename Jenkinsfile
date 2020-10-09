def NODE_NAME = 'AWS_Instance_CentOS'
def MAIL_TO = '$DEFAULT_RECIPIENTS'
def MAIL_SUBJECT = '[CI PGSpider] SQLite FDW Test FAILED ' + BRANCH_NAME
def BUILD_INFO = 'Jenkins job: ' + env.BUILD_URL
def BRANCH_NAME = 'Branch [' + env.BRANCH_NAME + ']'

def retrySh(String shCmd) {
    def MAX_RETRY = 10
    script {
        int status = 1;
        for (int i = 0; i < MAX_RETRY; i++) {
            status = sh(returnStatus: true, script: shCmd)
            if (status == 0) {
                echo "SUCCESS: " + shCmd
                break
            } else {
                echo "RETRY: " + shCmd
                sleep 5
            }
        }
        if (status != 0) {
            sh(shCmd)
        }
    }
}

pipeline {
    agent {
        node {
            label NODE_NAME
        }
    }
    options {
        gitLabConnection('GitLabConnection')
    }
    triggers {
        gitlab(
            triggerOnPush: true,
            triggerOnMergeRequest: false,
            triggerOnClosedMergeRequest: false,
            triggerOnAcceptedMergeRequest: true,
            triggerOnNoteRequest: false,
            setBuildDescription: true,
            branchFilterType: 'All',
            secretToken: "14edd1f2fc244d9f6dfc41f093db270a"
        )
    }
    stages {
        stage('Build') {
            steps {
                sh '''
                    rm -rf postgresql-13beta2 || true
                    tar -zxvf /home/jenkins/Postgres/postgresql-13beta2.tar.gz > /dev/null
                    cd postgresql-13beta2
                    ./configure --prefix=$(pwd)/install > /dev/null
                    make clean && make > /dev/null
                '''
                 dir("postgresql-13beta2/contrib") {
                    sh 'rm -rf duckdb_fdw || true'
                    retrySh('git clone -b ' + env.GIT_BRANCH + ' ' + env.GIT_URL)
                }
            }
            post {
                failure {
                    echo '** BUILD FAILED !!! NEXT STAGE WILL BE SKIPPED **'
                    emailext subject: "${MAIL_SUBJECT}", body: BUILD_INFO + "\nGit commit: " + env.GIT_URL.replace(".git", "/commit/") + env.GIT_COMMIT + "\n" + '${BUILD_LOG, maxLines=200, escapeHtml=false}', to: "${MAIL_TO}", attachLog: false
                    updateGitlabCommitStatus name: 'Build', state: 'failed'
                }
                success {
                    updateGitlabCommitStatus name: 'Build', state: 'success'
                }
            }
        }
        stage('duckdb_fdw_test') {
            steps {
                dir("postgresql-13beta2/contrib/duckdb_fdw") {
                    catchError() {
                        sh '''
                            chmod +x *.sh
                            ./test.sh
                        '''
                    }
                    script {
                        status = sh(returnStatus: true, script: "grep -q 'All [0-9]* tests passed' 'make_check.out'")
                        if (status != 0) {
                            unstable(message: "Set UNSTABLE result")
                            emailext subject: "${MAIL_SUBJECT}", body: BUILD_INFO + "\nGit commit: " + env.GIT_URL.replace(".git", "/commit/") + env.GIT_COMMIT + "\n" + '${FILE,path="make_check.out"}', to: "${MAIL_TO}", attachLog: false
                            sh 'cat regression.diffs || true'
                            updateGitlabCommitStatus name: 'duckdb_fdw_test', state: 'failed'
                        } else {
                            updateGitlabCommitStatus name: 'duckdb_fdw_test', state: 'success'
                        }
                    }
                }
            }
        }
        stage('duckdb_fdw_test_extra') {
            steps {
                dir("postgresql-13beta2/contrib/duckdb_fdw") {
                    catchError() {
                        sh '''
                            chmod +x *.sh
                            ./test_extra.sh
                        '''
                    }
                    script {
                        status = sh(returnStatus: true, script: "grep -q 'All [0-9]* tests passed' 'make_check.out'")
                        if (status != 0) {
                            unstable(message: "Set UNSTABLE result")
                            emailext subject: "${MAIL_SUBJECT}", body: BUILD_INFO + "\nGit commit: " + env.GIT_URL.replace(".git", "/commit/") + env.GIT_COMMIT + "\n" + '${FILE,path="make_check.out"}', to: "${MAIL_TO}", attachLog: false
                            sh 'cat regression.diffs || true'
                            updateGitlabCommitStatus name: 'duckdb_fdw_test_extra', state: 'failed'
                        } else {
                            updateGitlabCommitStatus name: 'duckdb_fdw_test_extra', state: 'success'
                        }
                    }
                }
            }
        }
    }
}