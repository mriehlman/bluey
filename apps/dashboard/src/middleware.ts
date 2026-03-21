export { default } from "next-auth/middleware";

export const config = {
  matcher: ["/predictions/:path*", "/simulator/:path*", "/events/:path*", "/settings/:path*"],
};
