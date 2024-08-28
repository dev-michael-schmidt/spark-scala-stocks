set -o allexport
source .env
set +o allexport
mvn clean compile assembly:single