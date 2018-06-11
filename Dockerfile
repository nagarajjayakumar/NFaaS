FROM yogeshprabhu/myhana

COPY pom.xml nfaas/

COPY Jenkinsfile nfaas/

COPY src/ nfaas/src/

COPY bin/ nfaas/bin/

COPY Servers nfaas/Servers

COPY nFaaS.iml nfaas/nFaaS.iml

WORKDIR nfaas/

RUN ["mvn", "install", "-Dmaven.test.skip=true"]

ENTRYPOINT [ "/bin/bash" ]
