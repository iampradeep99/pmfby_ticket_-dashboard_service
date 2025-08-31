# Use an official Node.js runtime as base image

FROM node:18


#RUN apk add --no-cache bash

# Set working directory
WORKDIR /app

# Copy package.json and install dependencies
COPY package*.json ./
RUN npm install

# Copy all source code
COPY . .

# Build the NestJS app
RUN npm run build

# Expose the port NestJS runs on
EXPOSE 3000

# Command to run the built app
CMD ["node", "dist/main"]
