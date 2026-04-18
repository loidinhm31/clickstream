FROM amazoncorretto:17

# Install required packages
RUN yum install -y procps && yum clean all

# Copy the application JAR
COPY target/spark-etl-1.0.0-SNAPSHOT.jar /app/app.jar

# Set working directory
WORKDIR /app

# Run the application
ENTRYPOINT ["java", \
    "--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED", \
    "-Djava.security.egd=file:/dev/./urandom", \
    "-jar", "app.jar"]
