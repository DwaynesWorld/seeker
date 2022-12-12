import Box from "@mui/material/Box";
import CssBaseline from "@mui/material/CssBaseline";
import Head from "next/head";
import Sidebar from "./sidebar";
import Toolbar from "./toolbar";

interface PageProps {
  title: string;
  meta?: string;
  children: React.ReactNode;
}

export default function Page({ title, meta, children }: PageProps) {
  return (
    <Box sx={{ display: "flex" }}>
      <CssBaseline />
      <Head>
        <title>{title}</title>
        <meta name="description" content={meta ?? ""} />
        <link rel="icon" href="/favicon.ico" />
      </Head>

      <Toolbar />
      <Sidebar />

      <Box
        component="main"
        sx={{ flexGrow: 1, bgcolor: "background.default", p: 3, mt: "45px" }}>
        {children}
      </Box>
    </Box>
  );
}
