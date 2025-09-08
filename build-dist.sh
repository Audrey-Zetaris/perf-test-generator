#!/bin/bash

#######################################
# Build and Package Distribution Script
#######################################

set -e  # Exit on error

echo "========================================="
echo "Building Delta Lake Performance Test Generator"
echo "========================================="

# Clean previous builds
echo "Cleaning previous builds..."
rm -rf target/universal/
rm -rf lib/*.jar

# Compile the project
echo "Compiling Scala sources..."
sbt clean compile

# Run tests
echo "Running tests..."
sbt test

# Build assembly JAR
echo "Building assembly JAR..."
sbt assembly

# Copy JAR to lib directory
echo "Copying JAR to lib directory..."
mkdir -p lib
cp target/scala-2.12/perf-test-generator-1.0.0.jar lib/

# Create distribution package
echo "Creating distribution packages..."
sbt universal:packageBin
sbt universal:packageZipTarball

echo ""
echo "========================================="
echo "Build Complete!"
echo "========================================="
echo ""
echo "Distribution packages created:"
echo "  - ZIP: target/universal/perf-test-generator-1.0.0.zip"
echo "  - TGZ: target/universal/perf-test-generator-1.0.0.tgz"
echo ""
echo "To extract and run:"
echo "  1. unzip target/universal/perf-test-generator-1.0.0.zip"
echo "  2. cd perf-test-generator-1.0.0"
echo "  3. ./bin/start-generator.sh"
echo ""