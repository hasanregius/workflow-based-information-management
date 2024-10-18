# RVPN import
require(readxl)
rvpn_dir = "/Users/315852/Downloads/rvpn RDC/"
flist = list.files(rvpn_dir, full.names = T)
rvpn = data.frame()
for (i in 1:length(flist)) {
  temp = read_xlsx(flist[i], trim_ws = T, skip = 1, sheet = 1, na = "", col_types = "text")
  names(temp) = c("test_id", "din", "volume", "row", "column", "d614g_ic50", "d614g_interpretation",
                  "ba45_ic50", "ba45_interpretation", "delta_ic50", "delta_interpretation",
                  "omicron_ic50", "omicron_interpretation")

  temp_ic50 = temp %>%
    select(din, d614g_ic50, ba45_ic50, delta_ic50, omicron_ic50) %>%
    pivot_longer(cols = ends_with('0'), names_to = "strain", values_to = "ic50") %>%
    mutate(
      strain = case_when(
        str_detect(strain, "d614g") ~ "D614G",
        str_detect(strain, "ba45") ~ "BA.4/5",
        str_detect(strain, "delta") ~ "delta",
        str_detect(strain, "omicron") ~ "omicron"),
      quantification = case_when(
        ic50 %in% c("<40","1:40") ~ "below_loq",
        ic50 %in% c("<40","1:40") == F ~ "within_loq"),
      ic50 = as.numeric(ic50)
      )

  temp_interpretation = temp %>%
    select(din, d614g_interpretation, ba45_interpretation, delta_interpretation, omicron_interpretation) %>%
    pivot_longer(cols = contains('interpretation'), names_to = "strain", values_to = "interpretation") %>%
    mutate(
      strain = case_when(
        str_detect(strain, "d614g") ~ "D614G",
        str_detect(strain, "ba45") ~ "BA.4/5",
        str_detect(strain, "delta") ~ "delta",
        str_detect(strain, "omicron") ~ "omicron"),
      interpretation = tolower(interpretation)
      )

  temp = temp_ic50 %>%
    select(din, strain, quantification, ic50) %>%
    full_join(., temp_interpretation, by = c("din", "strain"))

  rvpn = rbind(rvpn, temp)
}

# Graham added blank DINs so removing that before accessioning
rvpn = rvpn %>%
  filter(din %in% samples_db$din) %>%
  select(din,
         rvpn_strain = strain,
         rvpn_quantification = quantification,
         rvpn_ic50 = ic50,
         rvpn_interpretation = interpretation) %>%
  distinct()

# Accession to the database
dbWriteTable(sb_conn, "vri_rvpn", rvpn, append = T)
rvpn_db = dbReadTable(sb_conn, "vri_rvpn") %>%
  mutate(key = paste0(din, rvpn_strain))
rvpn = rvpn %>%
  mutate(key = paste0(din, rvpn_strain)) %>%
  filter(key %in% rvpn_db$key == F) %>%
  select(-key)
