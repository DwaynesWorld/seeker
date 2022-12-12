import { useState } from "react";
import { useTheme } from "@mui/material";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Typography from "@mui/material/Typography";
import Menu from "@mui/material/Menu";
import MenuItem from "@mui/material/MenuItem";
import ArrowDropDownRoundedIcon from "@mui/icons-material/ArrowDropDownRounded";

export default function ToolbarClusterPicker() {
  const theme = useTheme();
  const [anchor, setAnchor] = useState<null | HTMLElement>(null);
  const open = Boolean(anchor);

  const handleClick = (event: React.MouseEvent<HTMLElement>) => {
    setAnchor(event.currentTarget);
  };

  const handleClose = () => {
    setAnchor(null);
  };

  return (
    <Box display="flex" flexDirection="row">
      <Button
        aria-expanded={open ? "true" : undefined}
        aria-haspopup="true"
        sx={{
          textTransform: "none",
          px: "6px",
          py: "2px",
        }}
        onClick={handleClick}>
        <Box display="flex" flexDirection="column" alignItems="flex-start">
          <Typography
            variant="body2"
            component="p"
            fontSize={13}
            fontWeight={700}
            color={theme.palette.text.primary}>
            acme.factory.east-us
          </Typography>
          <Typography
            variant="caption"
            component="p"
            fontSize={10}
            color="green">
            Connected
          </Typography>
        </Box>
        <Box
          ml={3}
          display="flex"
          flexDirection="row"
          justifyContent="flex-end">
          <ArrowDropDownRoundedIcon />
        </Box>
      </Button>

      <Menu anchorEl={anchor} open={open} onClose={handleClose}>
        <MenuItem onClick={handleClose}>Menu Item 1</MenuItem>
      </Menu>
    </Box>
  );
}
