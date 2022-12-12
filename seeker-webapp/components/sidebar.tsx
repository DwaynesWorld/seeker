import Drawer from "@mui/material/Drawer";
import EarbudsTwoToneIcon from "@mui/icons-material/EarbudsTwoTone";
import FolderCopyTwoToneIcon from "@mui/icons-material/FolderCopyTwoTone";
import HubTwoToneIcon from "@mui/icons-material/HubTwoTone";
import Link from "next/link";
import List from "@mui/material/List";
import ListItem from "@mui/material/ListItem";
import ListItemButton from "@mui/material/ListItemButton";
import ListItemIcon from "@mui/material/ListItemIcon";
import ListItemText from "@mui/material/ListItemText";
import ListSubheader from "@mui/material/ListSubheader";
import PeopleTwoToneIcon from "@mui/icons-material/PeopleTwoTone";
import PublicTwoToneIcon from "@mui/icons-material/PublicTwoTone";
import SegmentTwoToneIcon from "@mui/icons-material/SegmentTwoTone";
import SettingsTwoToneIcon from "@mui/icons-material/SettingsTwoTone";

import { NavigationContent, NavigationSection } from "../types/app/navigation";
import { useTheme } from "@mui/material";
import { useRouter } from "next/router";

const DRAWER_WIDTH = 240;

const APP_NAVIGATION: NavigationContent = {
  sections: [
    {
      title: "Main",
      items: [
        {
          label: "Cluster Overview",
          link: "/",
          icon: (selected) => (
            <PublicTwoToneIcon
              fontSize="small"
              color={selected ? "primary" : "inherit"}
            />
          ),
        },
        {
          label: "Topics",
          link: "/topics",
          icon: (selected) => (
            <FolderCopyTwoToneIcon
              fontSize="small"
              color={selected ? "primary" : "inherit"}
            />
          ),
        },
        {
          label: "Consumer Groups",
          link: "/groups",
          icon: (selected) => (
            <HubTwoToneIcon
              fontSize="small"
              color={selected ? "primary" : "inherit"}
            />
          ),
        },
        {
          label: "Schema Registry",
          link: "/schemas",
          icon: (selected) => (
            <SegmentTwoToneIcon
              fontSize="small"
              color={selected ? "primary" : "inherit"}
            />
          ),
        },
        {
          label: "Kafka Connect",
          link: "/connect",
          icon: (selected) => (
            <EarbudsTwoToneIcon
              fontSize="small"
              color={selected ? "primary" : "inherit"}
            />
          ),
        },
      ],
    },
    {
      title: "Other",
      items: [
        {
          label: "Manage Team",
          link: "/teams",
          icon: (selected) => (
            <PeopleTwoToneIcon
              fontSize="small"
              color={selected ? "primary" : "inherit"}
            />
          ),
        },
        {
          label: "Settings",
          link: "/settings",
          icon: (selected) => (
            <SettingsTwoToneIcon
              fontSize="small"
              color={selected ? "primary" : "inherit"}
            />
          ),
        },
      ],
    },
  ],
};

interface SidebarProps {}

export default function Sidebar({}: SidebarProps) {
  return (
    <Drawer
      variant="permanent"
      anchor="left"
      sx={{
        width: DRAWER_WIDTH,
        flexShrink: 0,
        "& .MuiDrawer-paper": {
          width: DRAWER_WIDTH,
          boxSizing: "border-box",
          paddingTop: "45px", // equal to AppBar height
          border: 0,
        },
      }}
      open>
      {APP_NAVIGATION.sections.map((section, _) => (
        <SidebarSection key={section.title} section={section} />
      ))}
    </Drawer>
  );
}

interface SidebarSectionProps {
  section: NavigationSection;
}
function SidebarSection({ section }: SidebarSectionProps) {
  const theme = useTheme();
  const router = useRouter();

  console.log(router);

  return (
    <>
      <List
        dense
        subheader={
          <ListSubheader
            component="div"
            id="nested-list-subheader"
            sx={{
              fontSize: 11,
              fontWeight: "bold",
              textTransform: "uppercase",
              color: theme.palette.text.disabled,
              pt: 1,
              lineHeight: "30px",
            }}>
            {section.title}
          </ListSubheader>
        }>
        {section.items.map((item, _) => (
          <Link key={item.label} href={item.link}>
            <ListItem disablePadding>
              <ListItemButton
                sx={{ px: 1, mx: 1.5, minHeight: 32, borderRadius: "6px" }}
                selected={router.pathname == item.link}>
                <ListItemIcon sx={{ minWidth: 36 }}>
                  {item.icon(router.pathname == item.link)}
                </ListItemIcon>
                <ListItemText
                  primary={item.label}
                  primaryTypographyProps={{
                    fontSize: 13,
                    fontWeight: "bold",
                    color:
                      router.pathname == item.link
                        ? theme.palette.text.primary
                        : theme.palette.text.secondary,
                  }}
                />
              </ListItemButton>
            </ListItem>
          </Link>
        ))}
      </List>
    </>
  );
}
