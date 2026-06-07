/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  output: 'standalone',
  async rewrites() {
    return [
      {
        source: '/api/leaderboard/v1/:path*',
        destination: `${process.env.LEADERBOARD_API_URL ?? 'http://localhost:8081'}/api/:path*`,
      },
      {
        source: '/api/submission/v1/submissions',
        destination: `${process.env.SUBMISSION_API_URL ?? 'http://localhost:8082'}/submit`,
      },
      {
        source: '/api/submission/v1/submissions/:submission_id',
        destination: `${process.env.SUBMISSION_API_URL ?? 'http://localhost:8082'}/submissions/:submission_id`,
      },
      {
        source: '/api/submission/v1/submissions/:submission_id/benchmark',
        destination: `${process.env.SUBMISSION_API_URL ?? 'http://localhost:8082'}/submissions/:submission_id/benchmark`,
      },
      {
        source: '/api/submission/v1/:path*',
        destination: `${process.env.SUBMISSION_API_URL ?? 'http://localhost:8082'}/:path*`,
      },
      {
        source: '/api/auth/:path*',
        destination: `${process.env.AUTH_API_URL ?? 'http://localhost:8083'}/:path*`,
      },
    ];
  },
  images: {
    remotePatterns: [
      {
        protocol: 'https',
        hostname: 'lh3.googleusercontent.com',
      },
    ],
  },
};

export default nextConfig;
