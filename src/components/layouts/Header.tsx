import Image from "next/image";
import Link from "next/link";
import Logo_Dark from "@/assets/images/TSA_Logo_Primary_White_RGB 1.png";

const Header = () => {
  return (
    <header className="h-16 flex items-center border-b-2 border-border bg-bg-secondary transition-colors">
      <Link href={"/"}>
        <Image src={Logo_Dark} alt="tsa_logo_black" height={60} />
      </Link>
    </header>
  );
};

export default Header;