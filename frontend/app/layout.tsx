import "./globals.css";

export const metadata = {
  title: "Scout MVP",
  description: "Copilot → boolean strategy → find profiles"
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}

