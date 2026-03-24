export { default } from "next-auth/middleware";

export const config = {
  matcher: ["/predictions/:path*", "/simulator/:path*", "/pattern-simulator/:path*", "/events/:path*", "/settings/:path*"],
};
