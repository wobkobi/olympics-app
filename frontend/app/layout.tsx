"use client"; // Ensure this component is treated as a client component

import './globals.css';
import { ReactNode } from 'react';
import Image from 'next/image';
import { ResetProvider } from './context/ResetContext'; // Import the ResetProvider

interface RootLayoutProps {
  children: ReactNode;
}

export default function RootLayout({ children }: RootLayoutProps) {
  return (
    <html lang="en">
      <body className="bg-olympicWhite text-gray-900 min-h-screen flex flex-col">
        <ResetProvider>
          <header className="bg-olympicBlue text-white p-4 shadow-md">
            <div className="container mx-auto flex items-center">
              <Image
                src="/olympic_rings.svg"
                alt="Olympic Rings"
                width={40}
                height={40}
                className="h-10 w-10 mr-4"
              />
              <h1 className="text-3xl font-bold">Olympics Data Viewer</h1>
            </div>
          </header>
          <main className="container mx-auto flex-1 py-8">{children}</main>
          <footer className="bg-olympicBlack text-white text-center py-4 mt-8">
            <p>© {new Date().getFullYear()} Olympics Data Viewer</p>
          </footer>
        </ResetProvider>
      </body>
    </html>
  );
}
