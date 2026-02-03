FROM node:20-slim

WORKDIR /app

COPY package.json package-lock.json* tsconfig.json ./
RUN npm install

COPY src ./src
COPY supabase ./supabase
COPY README.md ./

ENTRYPOINT ["npm", "run", "ingest", "--"]
