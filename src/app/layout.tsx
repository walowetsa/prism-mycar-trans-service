import type { Metadata } from "next";
import "./globals.css";
import Header from "@/components/layouts/Header";
import { Open_Sans } from 'next/font/google'

const openSans = Open_Sans ({
  subsets: ['latin'],
  display: 'swap'
})

export const metadata: Metadata = {
  title: "TSAi | Call Analytics",
  description: "Call Analytics powered by TSAi",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body
        className={`${openSans.className} flex-1 bg-bg-primary min-h-screen max-h-screen overflow-hidden flex flex-col antialiased text-text-primary`}
      >
        <Header />
        {children}
      </body>
    </html>
  );
}
