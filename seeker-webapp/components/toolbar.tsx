import AppBar from "@mui/material/AppBar";
import Box from "@mui/material/Box";
import Grid from "@mui/material/Grid";
import Container from "@mui/material/Container";
import Avatar from "@mui/material/Avatar";
import Divider from "@mui/material/Divider";
import ToolbarClusterPicker from "./toolbar.cluster.picker";
import { Toolbar as MToolbar, useTheme } from "@mui/material";

interface ToolbarProps {}

export default function Toolbar({}: ToolbarProps) {
  const theme = useTheme();

  return (
    <AppBar
      position="fixed"
      elevation={0}
      sx={{
        bgcolor: "white",
        zIndex: theme.zIndex.drawer + 1,
        borderBottomColor: "lightgray",
        borderBottomWidth: 1,
        borderBottomStyle: "solid",
      }}>
      <Container disableGutters maxWidth={false}>
        <MToolbar disableGutters variant="dense">
          <Grid container direction="row" alignItems="center">
            <Grid item display="flex" flex={1}>
              <Grid
                item
                display="flex"
                justifyContent="center"
                alignItems="center">
                <PlaceholderIcon />
              </Grid>
              <Divider orientation="vertical" flexItem />
              <Grid item sx={{ mx: 1 }} alignItems="center">
                <ToolbarClusterPicker />
              </Grid>
            </Grid>

            <Grid item display="flex" flex={1} justifyContent="flex-end">
              <Box sx={{ mx: 1.5, p: 0.5 }}>
                <Avatar
                  src="https://i.pravatar.cc/400?img=12"
                  sx={{ width: 32, height: 32 }}
                />
              </Box>
            </Grid>
          </Grid>
        </MToolbar>
      </Container>
    </AppBar>
  );
}

function PlaceholderIcon() {
  const theme = useTheme();
  return (
    <Box
      sx={{
        mx: 1.5,
        p: 0.5,
        height: 32,
        width: 32,
        backgroundColor: theme.palette.primary.main,
        borderRadius: "6px",
      }}
    />
  );
}
